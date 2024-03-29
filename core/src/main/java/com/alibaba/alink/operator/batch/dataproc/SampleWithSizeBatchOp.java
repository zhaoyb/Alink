package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.probabilistic.XRandom;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SampleWithSizeParams;

import java.util.Arrays;

/**
 * Sample the input data with given size with or without replacement.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@NameCn("固定条数随机采样")
@NameEn("Data Sampling With Fixed Size")
public class SampleWithSizeBatchOp extends BatchOperator <SampleWithSizeBatchOp>
	implements SampleWithSizeParams <SampleWithSizeBatchOp> {

	private static final long serialVersionUID = -4682487914099349334L;

	public SampleWithSizeBatchOp() {
		this(new Params());
	}

	public SampleWithSizeBatchOp(Params params) {
		super(params);
	}

	public SampleWithSizeBatchOp(int numSamples) {
		this(numSamples, false);
	}

	public SampleWithSizeBatchOp(int numSamples, boolean withReplacement) {
		this(new Params()
			.set(SIZE, numSamples)
			.set(WITH_REPLACEMENT, withReplacement)
		);
	}

	@Override
	public SampleWithSizeBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final boolean withReplacement = getWithReplacement();
		final int numSamples = getSize();

		DataSet <Tuple2 <Integer, Row>> dataSetWith = in.getDataSet()
			.map(new RichMapFunction <Row, Tuple2 <Integer, Row>>() {
				private int taskId;

				@Override
				public void open(Configuration parameters) throws Exception {
					this.taskId = this.getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public Tuple2 <Integer, Row> map(Row value) throws Exception {
					return Tuple2.of(taskId, value);
				}
			});
		DataSet <Tuple2 <int[], long[]>> sampleCountsByTask = dataSetWith
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple3 <Integer, Integer, Long>>() {
				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
										 Collector <Tuple3 <Integer, Integer, Long>> out)
					throws Exception {
					long localCount = 0;
					for (Tuple2 <Integer, Row> val : values) {
						localCount++;
					}
					out.collect(Tuple3.of(this.getRuntimeContext().getIndexOfThisSubtask(),
						this.getRuntimeContext().getNumberOfParallelSubtasks(),
						localCount));

				}
			})
			.reduceGroup(new RichGroupReduceFunction <Tuple3 <Integer, Integer, Long>, Tuple2 <int[], long[]>>() {
				@Override
				public void reduce(Iterable <Tuple3 <Integer, Integer, Long>> values,
								   Collector <Tuple2 <int[], long[]>> out)
					throws Exception {
					long[] countsByTask = null;
					long totalCount = 0;
					int numTask = 0;
					for (Tuple3 <Integer, Integer, Long> t3 : values) {
						numTask = t3.f1;
						if (countsByTask == null) {
							countsByTask = new long[numTask];
						}
						countsByTask[t3.f0] = t3.f2;
						totalCount += t3.f2;
					}

					int[] sampleCountsByTask = null;
					if (!withReplacement && (totalCount < numSamples)) {
						sampleCountsByTask = new int[countsByTask.length];
						for (int i = 0; i < sampleCountsByTask.length; i++) {
							sampleCountsByTask[i] = (int)countsByTask[i];
						}
					} else {
						sampleCountsByTask = getSampleCountByTask(totalCount, countsByTask, numSamples);
					}

					// check
					int sum = 0;
					for (int j : sampleCountsByTask) {
						sum += j;
					}
					if (sum != Math.min(numSamples, totalCount)) {
						throw new AkIllegalStateException("sampleCountsByTask not equal with numSamples.");
					}

					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("sampleCountsByTask: " + JsonConverter.toJson(sampleCountsByTask));
						System.out.println("countsByTask: " + JsonConverter.toJson(countsByTask));
					}

					out.collect(Tuple2.of(sampleCountsByTask, countsByTask));
				}
			});

		DataSet <Row> sampleDataSet = dataSetWith
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Row>() {
				int[] sampleCountsByTask;
				long[] countsByTask;

				@Override
				public void open(Configuration parameters) throws Exception {
					Tuple2 <int[], long[]> t2 = (Tuple2) this.getRuntimeContext()
						.getBroadcastVariable("sampleCountsByTask")
						.get(0);

					this.sampleCountsByTask = t2.f0;
					this.countsByTask = t2.f1;
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Row> out)
					throws Exception {
					int taskId = this.getRuntimeContext().getIndexOfThisSubtask();
					int dataTaskId = -1;

					int[] indices = null;
					int idx = 0;
					int indicesIdx = 0;
					int sampleIdx = -1;
					int localSampleSize = -1;
					for (Tuple2 <Integer, Row> t2 : values) {
						if (dataTaskId == -1) {
							dataTaskId = t2.f0;
							localSampleSize = this.sampleCountsByTask[dataTaskId];
							long localCount = this.countsByTask[dataTaskId];
							XRandom random = new XRandom();
							random.setSeed(System.currentTimeMillis());
							indices = random.randInts((int) localCount, localSampleSize, withReplacement);

							if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
								System.out.println("indices cut: " + indices.length);
							}

							Arrays.sort(indices);
							sampleIdx = indices[0];
						}
						if (t2.f0 != dataTaskId) {
							throw new AkIllegalStateException("dataTaskId has changed.");
						}
						if (sampleIdx == idx) {
							while (indicesIdx < localSampleSize && indices[indicesIdx] == sampleIdx) {
								out.collect(t2.f1);
								indicesIdx++;
							}
							if (indicesIdx == localSampleSize) {
								break;
							} else {
								if (indices[indicesIdx] != sampleIdx) {
									sampleIdx = indices[indicesIdx];
								}
							}
						}
						idx++;
					}
				}
			})
			.withBroadcastSet(sampleCountsByTask, "sampleCountsByTask");

		this.setOutput(sampleDataSet, in.getSchema());

		return this;
	}

	protected static int[] getSampleCountByTask(long totalCount, long[] countsByTask, long sampleCount) {
		double ratio = (double) sampleCount / totalCount;
		int numTask = countsByTask.length;
		int[] sampleCountsByTask = new int[numTask];
		long curSampleCount = 0;
		for (int i = 0; i < numTask; i++) {
			sampleCountsByTask[i] = (int) (countsByTask[i] * ratio);
			curSampleCount += sampleCountsByTask[i];
		}

		for (int i = 0; i < (sampleCount - curSampleCount); i++) {
			sampleCountsByTask[i]++;
		}
		return sampleCountsByTask;
	}

}
