package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp.MultiQuantile;
import com.alibaba.alink.operator.common.feature.quantile.PairComparable;
import com.alibaba.alink.operator.common.tree.Preprocessing;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.statistics.HasRoundMode;
import com.alibaba.alink.params.statistics.QuantileParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp.quantilePreparing;

/**
 * In statistics and probability quantiles are cut points dividing the range of a probability distribution into
 * contiguous intervals with equal probabilities, or dividing the observations in a sample in the same way.
 * (https://en.wikipedia.org/wiki/Quantile)
 * <p>
 * reference: Yang, X. (2014). Chong gou da shu ju tong ji (1st ed., pp. 25-29).
 * <p>
 * Note: This algorithm is improved on the base of the parallel sorting by regular sampling(PSRS). The following step is
 * added to the PSRS
 * <ul>
 * <li>replace (val) with (val, task id) to distinguishing the
 * same value on different machines</li>
 * <li>
 * the index of q-quantiles: index = roundMode((n - 1) * k / q),
 * n is the count of sample, k satisfying 0 < k < q
 * </li>
 * </ul>
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("分位数")
@NameEn("Quantile")
public final class QuantileBatchOp extends BatchOperator <QuantileBatchOp>
	implements QuantileParams <QuantileBatchOp> {

	private static final long serialVersionUID = -86119177892147044L;

	private static final Logger LOG = LoggerFactory.getLogger(QuantileBatchOp.class);

	public QuantileBatchOp() {
		super(null);
	}

	public QuantileBatchOp(Params params) {
		super(params);
	}

	@Override
	public QuantileBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String[] quantileColNames = getParams().get(HasSelectedColsDefaultAsNull.SELECTED_COLS);

		if (quantileColNames == null) {

			if (getParams().get(HasSelectedColDefaultAsNull.SELECTED_COL) == null) {
				throw new AkIllegalArgumentException("There must select one or more colum in quantile batch op.");
			}

			quantileColNames = new String[] {getParams().get(HasSelectedColDefaultAsNull.SELECTED_COL)};
		}

		TypeInformation <?>[] quantileColTypes
			= TableUtil.findColTypesWithAssertAndHint(in.getSchema(), quantileColNames);

		final int[] quantileNum = new int[quantileColNames.length];
		Arrays.fill(quantileNum, getQuantileNum());

		final HasRoundMode.RoundMode roundMode = getRoundMode();

		DataSet <Row> input = Preprocessing.select(in, quantileColNames).getDataSet();

		Tuple4 <DataSet <PairComparable>, DataSet <Tuple2 <Integer, Long>>,
			DataSet <Long>, DataSet <Tuple2 <Integer, Long>>> quantileData =
			quantilePreparing(input, getParams().get(Preprocessing.ZERO_AS_MISSING));

		/* calculate quantile */
		DataSet <Tuple2 <Integer, Number>> quantileTuple = quantileData.f0
			.mapPartition(new MultiQuantile(quantileNum, roundMode, true))
			.withBroadcastSet(quantileData.f1, "counts")
			.withBroadcastSet(quantileData.f2, "totalCnt")
			.withBroadcastSet(quantileData.f3, "missingCounts");

		DataSet <Tuple3 <Integer, Number, Integer>> indexedQuantile = quantileTuple
			.partitionByHash(0)
			.mapPartition(new GiveQuantileIndices());

		DataSet <Row> quantile = indexedQuantile
			.partitionByHash(2)
			.mapPartition(new SerializeOutput(quantileNum))
			.withBroadcastSet(DataSetUtils.countElementsPerPartition(indexedQuantile).sum(1), "count");

		String[] outputColNames = new String[quantileColNames.length + 1];
		TypeInformation <?>[] outputColTypes = new TypeInformation <?>[quantileColNames.length + 1];

		outputColNames[0] = "quantile";
		outputColTypes[0] = AlinkTypes.LONG;

		System.arraycopy(quantileColNames, 0, outputColNames, 1, quantileColNames.length);
		System.arraycopy(quantileColTypes, 0, outputColTypes, 1, quantileColNames.length);

		/* set output */
		setOutput(quantile, outputColNames, outputColTypes);

		return this;
	}

	private static class SerializeOutput
		extends RichMapPartitionFunction <Tuple3 <Integer, Number, Integer>, Row> {
		private final int[] quantileNum;
		private long cnt;

		public SerializeOutput(int[] quantileNum) {this.quantileNum = quantileNum;}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			cnt = getRuntimeContext()
				.getBroadcastVariableWithInitializer(
					"count",
					new BroadcastVariableInitializer <Tuple2 <Integer, Long>, Long>() {
						@Override
						public Long initializeBroadcastVariable(Iterable <Tuple2 <Integer, Long>> data) {
							return data.iterator().next().f1;
						}
					}
				);
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, Number, Integer>> values, Collector <Row> out) {
			if (cnt <= 0L) {
				if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
					Row row = new Row(quantileNum.length + 1);
					for (int i = 0; i <= quantileNum[0]; ++i) {
						row.setField(0, (long) i);
						for (int j = 0; j < quantileNum.length; ++j) {
							row.setField(j + 1, null);
						}
						out.collect(row);
					}
				}

				return;
			}

			List <Tuple3 <Integer, Number, Integer>> buffer = new ArrayList <>();

			for (Tuple3 <Integer, Number, Integer> value : values) {
				buffer.add(value);
			}

			if (buffer.isEmpty()) {
				return;
			}

			buffer.sort(Comparator.comparing(o -> o.f2));
			Row row = new Row(quantileNum.length + 1);
			int size = buffer.size();

			for (int i = 0; i < size + 1; ++i) {
				Tuple3 <Integer, Number, Integer> prev;
				Tuple3 <Integer, Number, Integer> next;

				if (i == 0) {
					prev = buffer.get(i);
				} else {
					prev = buffer.get(i - 1);
				}

				if (i == size) {
					next = buffer.get(i - 1);
				} else {
					next = buffer.get(i);
				}

				if (i == size || !Objects.equals(next.f2, prev.f2)) {
					out.collect(row);
				}

				if (i == 0 || !Objects.equals(next.f2, prev.f2)) {
					row.setField(0, Long.valueOf(next.f2));
					for (int j = 0; j < quantileNum.length; ++j) {
						row.setField(j + 1, null);
					}
				}

				row.setField(next.f0 + 1, next.f1);
			}
		}
	}

	private static class GiveQuantileIndices
		implements MapPartitionFunction <Tuple2 <Integer, Number>, Tuple3 <Integer, Number, Integer>> {
		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Number>> values,
								 Collector <Tuple3 <Integer, Number, Integer>> out) {
			List <PairComparable> buffer = new ArrayList <>();

			for (Tuple2 <Integer, Number> value : values) {
				PairComparable pairComparable = new PairComparable();
				pairComparable.first = value.f0;
				pairComparable.second = value.f1;
				buffer.add(pairComparable);
			}

			buffer.sort(PairComparable::compareTo);

			int latest = -1;
			int quantile = 0;
			for (PairComparable pairComparable : buffer) {
				if (latest != pairComparable.first) {
					latest = pairComparable.first;
					quantile = 0;
				}
				out.collect(Tuple3.of(pairComparable.first, pairComparable.second, quantile));
				quantile++;
			}

		}
	}
}
