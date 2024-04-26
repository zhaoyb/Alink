package com.alibaba.alink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.sink.AkSinkLocalOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.PipelineModelParams;
import com.alibaba.alink.params.io.ModelFileSinkParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_DATA_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_STAT_ENABLED;
import static com.alibaba.alink.pipeline.PipelineModel.getOutSchema;

@NameCn("逐步训练的Pipeline")
@Internal
public class PipelineWithStepTrain implements WithParams <PipelineWithStepTrain>,
	ModelFileSinkParams <PipelineWithStepTrain>,
	HasMLEnvironmentId <PipelineWithStepTrain>, Cloneable, Serializable {
	protected Params params;

	ArrayList <PipelineStageBase <?>> stages = new ArrayList <>();

	public PipelineWithStepTrain() {
		this(new Params());
	}

	public PipelineWithStepTrain(Params params) {
		this.params = (null == params) ? new Params() : params;
	}

	public PipelineWithStepTrain(PipelineStageBase <?>... stages) {
		this.params = new Params();
		if (null != stages) {
			this.stages.addAll(Arrays.asList(stages));
		}
	}

	@Override
	public Params getParams() {
		if (null == this.params) {
			this.params = new Params();
		}
		return this.params;
	}

	@Override
	public PipelineWithStepTrain clone() throws CloneNotSupportedException {
		PipelineWithStepTrain pipeline = new PipelineWithStepTrain();
		for (PipelineStageBase <?> stage : this.stages) {
			pipeline.add(stage.clone());
		}
		return pipeline;
	}

	/**
	 * Appends the specified stage to the end of this pipeline.
	 *
	 * @param stage pipelineStage to be appended to this pipeline
	 * @return this pipeline
	 */
	public PipelineWithStepTrain add(PipelineStageBase <?> stage) {
		this.stages.add(stage);
		return this;
	}

	/**
	 * Inserts the specified stage at the specified position in this pipeline. Shifts the stage currently at that
	 * position (if any) and any subsequent stages to the right (adds one to their indices).
	 *
	 * @param index index at which the specified stage is to be inserted
	 * @param stage pipelineStage to be inserted
	 * @return this pipeline
	 * @throws IndexOutOfBoundsException
	 */
	@Deprecated
	public PipelineWithStepTrain add(int index, PipelineStageBase <?> stage) {
		this.stages.add(index, stage);
		return this;
	}

	/**
	 * Removes the stage at the specified position in this pipeline. Shifts any subsequent stages to the left
	 * (subtracts
	 * one from their indices).
	 *
	 * @param index the index of the stage to be removed
	 * @return the pipeline after remove operation
	 * @throws IndexOutOfBoundsException
	 */
	@Deprecated
	public PipelineWithStepTrain remove(int index) {
		this.stages.remove(index);
		return this;
	}

	/**
	 * Returns the stage at the specified position in this pipeline.
	 *
	 * @param index index of the stage to return
	 * @return the stage at the specified position in this pipeline
	 * @throws IndexOutOfBoundsException
	 */
	public PipelineStageBase <?> get(int index) {
		return this.stages.get(index);
	}

	/**
	 * Returns the number of stages in this pipeline.
	 *
	 * @return the number of stages in this pipeline
	 */
	public int size() {
		return this.stages.size();
	}

	/**
	 * Train the pipeline with batch data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	public PipelineModel fit(BatchOperator <?> input) {
		PipelineModel model = new PipelineModel(fit(input, false).f0)
			.setMLEnvironmentId(input.getMLEnvironmentId());

		model.getParams().set(PipelineModelParams.TRAINING_DATA_SCHEMA, TableUtil.schema2SchemaStr(input.getSchema()));
		return model;
	}

	public BatchOperator <?> fitAndTransform(BatchOperator <?> input) {
		return fit(input, true).f1;
	}

	private TransformerBase <?> stepFit(EstimatorBase <?, ?> stage, BatchOperator <?> data) {
		try {
			ModelBase <?> model = stage.fit(data);
			List <Row> modelRows = model.getModelData().collect();
			model.setModelData(new MemSourceBatchOp(modelRows, model.getModelData().getSchema()));
			return model;
		} catch (Exception e) {
			throw new RuntimeException("stepFit in pipeline failed.", e);
		}
	}

	private Tuple2 <TransformerBase <?>[], BatchOperator <?>> fit(BatchOperator <?> input, boolean withTransform) {
		for (PipelineStageBase <?> stage : stages) {
			if ((stage instanceof Trainer)
				&& EstimatorTrainerCatalog.lookupBatchTrainer(stage.getClass().getName()) == null
			) {
				throw new AkIllegalOperationException(
					"Pipeline can't fit BatchOperator, for the Estimator(" + stage.getClass().getName()
						+ ") not support.");
			}
		}
		TableSchema initSchema = input.getSchema();
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase <?>[] transformers = new TransformerBase <?>[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i);

			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = stepFit((EstimatorBase <?, ?>) stage, input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase <?>) stage;
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase <?>) stage;
			}

			if (i < lastEstimatorIdx || withTransform) {
				// temporarily disable lazy print transform results
				Boolean lazyPrintTransformDataEnabled = (Boolean) transformers[i].get(
					LAZY_PRINT_TRANSFORM_DATA_ENABLED);
				Boolean lazyPrintTransformStatEnabled = (Boolean) transformers[i].get(
					LAZY_PRINT_TRANSFORM_STAT_ENABLED);
				transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, false);
				transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, false);

				input = transformers[i].transform(input);

				transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, lazyPrintTransformDataEnabled);
				transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, lazyPrintTransformStatEnabled);
			}
		}
		getOutSchema(new PipelineModel(transformers), initSchema);
		return new Tuple2 <>(transformers, input);
	}

	/**
	 * Train the pipeline with batch data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	public PipelineModel fit(LocalOperator <?> input) {
		PipelineModel model = new PipelineModel(fit(input, false).f0);

		model.getParams().set(PipelineModelParams.TRAINING_DATA_SCHEMA, TableUtil.schema2SchemaStr(input.getSchema()));
		return model;
	}

	public LocalOperator <?> fitAndTransform(LocalOperator <?> input) {
		return fit(input, true).f1;
	}

	private Tuple2 <TransformerBase <?>[], LocalOperator <?>> fit(LocalOperator <?> input, boolean withTransform) {
		for (PipelineStageBase <?> stage : stages) {
			if ((stage instanceof Trainer)
				&& EstimatorTrainerCatalog.lookupLocalTrainer(stage.getClass().getName()) == null
			) {
				throw new AkIllegalOperationException(
					"Pipeline can't fit LocalOperator, for the Estimator(" + stage.getClass().getName()
						+ ") not support.");
			}
		}
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase <?>[] transformers = new TransformerBase <?>[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i);

			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase <?, ?>) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase <?>) stage;
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase <?>) stage;
			}

			if (i < lastEstimatorIdx || withTransform) {
				// temporarily disable lazy print transform results
				Boolean lazyPrintTransformDataEnabled = (Boolean) transformers[i].get(
					LAZY_PRINT_TRANSFORM_DATA_ENABLED);
				Boolean lazyPrintTransformStatEnabled = (Boolean) transformers[i].get(
					LAZY_PRINT_TRANSFORM_STAT_ENABLED);
				transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, false);
				transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, false);

				input = transformers[i].transform(input);

				transformers[i].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, lazyPrintTransformDataEnabled);
				transformers[i].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, lazyPrintTransformStatEnabled);
			}
		}
		return new Tuple2 <>(transformers, input);
	}

	/**
	 * Train the pipeline with stream data.
	 *
	 * @param input input data
	 * @return pipeline model
	 */
	public PipelineModel fit(StreamOperator <?> input) {
		int lastEstimatorIdx = getIndexOfLastEstimator();
		TransformerBase <?>[] transformers = new TransformerBase <?>[stages.size()];
		for (int i = 0; i < stages.size(); i++) {
			PipelineStageBase <?> stage = stages.get(i);
			if (i <= lastEstimatorIdx) {
				if (stage instanceof EstimatorBase) {
					transformers[i] = ((EstimatorBase <?, ?>) stage).fit(input);
				} else if (stage instanceof TransformerBase) {
					transformers[i] = (TransformerBase <?>) stage;
				}
				if (i < lastEstimatorIdx) {
					input = transformers[i].transform(input);
				}
			} else {
				// After lastEstimatorIdx, there're only Transformer stages, so it's safe to do type cast.
				transformers[i] = (TransformerBase <?>) stage;
			}
		}
		return new PipelineModel(transformers).setMLEnvironmentId(input.getMLEnvironmentId());
	}

	/**
	 * Get the index of the last estimator stage. If no estimator found, -1 is returned.
	 *
	 * @return index of the last estimator.
	 */
	private int getIndexOfLastEstimator() {
		int index = -1;
		for (int i = 0; i < stages.size(); i++) {
			if (stages.get(i) instanceof EstimatorBase) {
				index = i;
			}
		}
		return index;
	}

	/**
	 * Save the pipeline to a path using ak file.
	 */
	public void save(String path) {
		save(path, false);
	}

	public void save(String path, boolean overwrite) {
		save(new FilePath(path), overwrite);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
	 */
	public void save(FilePath filePath) {
		save(filePath, false);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
	 */
	public void save(FilePath filePath, boolean overwrite) {
		save(filePath, overwrite, 1);
	}

	/**
	 * Save the pipeline to a filepath using ak file.
	 */
	public void save(FilePath filePath, boolean overwrite, int numFiles) {
		save(filePath, overwrite, numFiles, "auto");
	}

	public void save(FilePath filePath, boolean overwrite, int numFiles, String mode) {
		mode = mode.toLowerCase();
		if (mode.equals("batch")) {
			saveBatch(filePath, overwrite, numFiles);
		} else if (mode.equals("local")) {
			saveLocal(filePath, overwrite, numFiles);
		} else if (mode.equals("auto")) {
			Tuple2 <Boolean, Boolean> t2 =
				PipelineModel.checkModels(this.stages.toArray(new PipelineStageBase <?>[0]));
			boolean containModel = t2.f0;
			boolean useBatchMode = t2.f1;
			if (containModel && useBatchMode) {
				saveBatch(filePath, overwrite, numFiles);
			} else {
				saveLocal(filePath, overwrite, numFiles);

				// Note: this will cause BatchOperator#execute to submit a new meaningless job when no other sinks.
				ModelExporterUtils.createEmptyBatchSourceSink(getMLEnvironmentId());
			}
		} else {
			throw new AkIllegalOperationException("Not support this save mode : " + mode);
		}
	}

	private void saveBatch(FilePath filePath, boolean overwrite, int numFiles) {
		saveBatch().link(
			new AkSinkBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setFilePath(filePath)
				.setOverwriteSink(overwrite)
				.setNumFiles(numFiles)
		);
	}

	private void saveLocal(FilePath filePath, boolean overwrite, int numFiles) {
		saveLocal().link(
			new AkSinkLocalOp()
				.setFilePath(filePath)
				.setOverwriteSink(overwrite)
				.setNumFiles(numFiles)
		);
	}

	/**
	 * Pack the pipeline to a BatchOperator.
	 */
	@Deprecated
	public BatchOperator <?> save() {
		return saveBatch();
	}

	private BatchOperator <?> saveBatch() {
		return ModelExporterUtils.serializePipelineStages(stages, params);
	}

	public LocalOperator <?> saveLocal() {
		return ModelExporterUtils.serializePipelineStagesLocal(stages, params);
	}

	public static PipelineWithStepTrain loadLocal(LocalOperator <?> localOp) {
		return new PipelineWithStepTrain(
			ModelExporterUtils.fillPipelineStages(
				localOp,
				ModelExporterUtils.collectMetaFromOp(localOp).f0,
				localOp.getSchema()
			).toArray(new PipelineStageBase[0]));
	}

	/**
	 * Load the pipeline from a path.
	 */
	public static PipelineWithStepTrain load(String path) {
		return load(new FilePath(path));
	}

	public static PipelineWithStepTrain load(FilePath filePath) {
		Tuple2 <TableSchema, Row> schemaAndMeta = ModelExporterUtils.loadMetaFromAkFile(filePath);

		return new PipelineWithStepTrain(
			ModelExporterUtils.fillPipelineStages(
				new ModelPipeFileData(filePath),
				ModelExporterUtils.deserializePipelineStagesFromMeta(
					schemaAndMeta.f1, schemaAndMeta.f0
				),
				schemaAndMeta.f0
			).toArray(new PipelineStageBase[0])
		);
	}

	public static PipelineWithStepTrain collectLoad(BatchOperator <?> batchOp) {
		return new PipelineWithStepTrain(
			ModelExporterUtils.fillPipelineStages(
				batchOp,
				ModelExporterUtils.collectMetaFromOp(batchOp).f0,
				batchOp.getSchema()
			).toArray(new PipelineStageBase[0]));
	}

	@Deprecated
	public static PipelineWithStepTrain load(FilePath filePath, Long mlEnvId) {
		Tuple2 <TableSchema, Row> schemaAndMeta = ModelExporterUtils.loadMetaFromAkFile(filePath);

		return new PipelineWithStepTrain(
			ModelExporterUtils.fillPipelineStages(
				new AkSourceBatchOp()
					.setFilePath(filePath)
					.setMLEnvironmentId(mlEnvId),
				ModelExporterUtils.deserializePipelineStagesFromMeta(
					schemaAndMeta.f1, schemaAndMeta.f0
				),
				schemaAndMeta.f0
			).toArray(new PipelineStageBase[0])
		);
	}

}
