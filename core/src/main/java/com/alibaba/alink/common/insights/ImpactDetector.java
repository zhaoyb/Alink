package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ImpactDetector {
	double threshold;

	String[] colNames;
	Map <String, Integer> colNameMap;
	Map <Object, Double>[] valueMaps;

	public ImpactDetector(double threshold) {
		this.threshold = threshold;
	}

	public void detect(LocalOperator <?> table, int threadNum) {
		detect(table, BreakdownDetector.getBreakdownCols(table.getSchema()), threadNum);
	}

	public void detect(LocalOperator <?> table, String[] breakdownCols, int threadNum) {
		colNames = breakdownCols;
		System.out.println("subspace col num: " + colNames.length);
		colNameMap = new HashMap <>(colNames.length);
		valueMaps = new Map[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			colNameMap.put(colNames[i], i);
			valueMaps[i] = new HashMap <>();
		}
		final int totalCount = table.getOutputTable().getNumRow();
		final int minCount = (int) Math.round(totalCount * this.threshold);

		int breakDownColNum = breakdownCols.length;

		if (threadNum == 1) {
			for (int j = 0; j < breakDownColNum; j++) {
				Map <Object, MutableInteger> maps = groupCount(table, colNames[j]);
				for (Map.Entry <Object, MutableInteger> entry : maps.entrySet()) {
					if (entry.getValue().getValue() > minCount) {
						valueMaps[j].put(entry.getKey(),
							entry.getValue().getValue() / (double) totalCount);
					}
				}
			}
		} else {
			final TaskRunner taskRunner = new TaskRunner();
			for (int i = 0; i < threadNum; ++i) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, threadNum, breakDownColNum);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, threadNum, breakDownColNum);

				if (cnt <= 0) {continue;}

				taskRunner.submit(() -> {
					for (int j = start; j < Math.min(start + cnt, breakDownColNum); j++) {
						Map <Object, MutableInteger> maps = groupCount(table, colNames[j]);
						for (Map.Entry <Object, MutableInteger> entry : maps.entrySet()) {
							if (entry.getValue().getValue() > minCount) {
								valueMaps[j].put(entry.getKey(),
									entry.getValue().getValue() / (double) totalCount);
							}
						}
					}
				});
			}

			taskRunner.join();
		}
	}

	public void reDetect(double newImpactThreshold) {
		if (newImpactThreshold > threshold) {
			Map <Object, Double>[] newValueMaps = new Map[colNames.length];
			for (int i = 0; i < colNames.length; i++) {
				newValueMaps[i] = new HashMap <>();
			}
			for (int j = 0; j < colNames.length; j++) {
				for (Entry <Object, Double> entry : valueMaps[j].entrySet()) {
					if (entry.getValue() >= newImpactThreshold) {
						newValueMaps[j].put(entry.getKey(), entry.getValue());
					}
				}
			}
			this.valueMaps = newValueMaps;
			this.threshold = newImpactThreshold;
		}
	}

	public static Map <Object, MutableInteger> groupCount(LocalOperator <?> in, String colName) {
		Map <Object, MutableInteger> maps = new HashMap <Object, MutableInteger>();
		int colIdx = TableUtil.findColIndex(in.getSchema(), colName);
		MTable mt = in.getOutputTable();
		if (mt.getNumRow() == 0) {
			return maps;
		}

		for (Row row : mt.getRows()) {
			Object val = row.getField(colIdx);
			if (val != null) {
				MutableInteger initValue = new MutableInteger(1);
				MutableInteger oldValue = maps.put(val, initValue);
				if (oldValue != null) {
					initValue.setValue(oldValue.getValue() + 1);
				}
			}
		}

		return maps;
	}

	public static Map <Object, MutableInteger> groupCount(LocalOperator <?> in, String colName, int fromId, int toId) {
		Map <Object, MutableInteger> maps = new HashMap <Object, MutableInteger>();
		int colIdx = TableUtil.findColIndex(in.getSchema(), colName);
		MTable mt = in.getOutputTable();
		if (mt.getNumRow() == 0) {
			return maps;
		}

		for (int rowIdx = fromId; rowIdx < toId; rowIdx++) {
			Object val = mt.getEntry(rowIdx, colIdx);
			if (val != null) {
				MutableInteger initValue = new MutableInteger(1);
				MutableInteger oldValue = maps.put(val, initValue);
				if (oldValue != null) {
					initValue.setValue(oldValue.getValue() + 1);
				}
			}
		}

		return maps;
	}

	public static Map <Object, MutableInteger> groupCountMultiThread(LocalOperator <?> in,
																	 String colName,
																	 int threadNum) {
		Map <Object, MutableInteger>[] maps = new Map[threadNum];
		int rowNum = in.getOutputTable().getNumRow();
		final TaskRunner taskRunner = new TaskRunner();

		for (int i = 0; i < threadNum; ++i) {
			final int threadId = i;
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, threadNum, rowNum);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, threadNum, rowNum);

			if (cnt <= 0) {continue;}

			taskRunner.submit(() -> {
				maps[threadId] = groupCount(in, colName, start, start + cnt);
			});
		}

		taskRunner.join();

		// merge
		Map <Object, MutableInteger> outMap = new HashMap <>();
		for (int i = 0; i < threadNum; i++) {
			for (Map.Entry <Object, MutableInteger> entry : maps[i].entrySet()) {
				MutableInteger e2 = entry.getValue();
				MutableInteger oldValue = outMap.put(entry.getKey(), e2);
				if (oldValue != null) {
					e2.setValue(oldValue.getValue() + e2.getValue());
				}
			}
		}
		return outMap;
	}

	public double predict(Subspace subspace) {
		Integer k = colNameMap.get(subspace.colName);
		if (null != k) {
			Double value = valueMaps[k].get(subspace.value);
			if (null != value) {
				return value;
			}
		}
		return 0.0;
	}

	public List <Tuple2 <Subspace, Double>> listSingleSubspace() {
		List <Tuple2 <Subspace, Double>> result = new ArrayList <>();

		for (Entry <String, Integer> name_index : colNameMap.entrySet()) {
			String colName = name_index.getKey();
			Map <Object, Double> valueMap = valueMaps[name_index.getValue()];
			for (Entry <Object, Double> value_impact : valueMap.entrySet()) {
				result.add(Tuple2.of(new Subspace(colName, value_impact.getKey()), value_impact.getValue()));
			}
		}

		Collections.sort(result, new Comparator <Tuple2 <Subspace, Double>>() {
			@Override
			public int compare(Tuple2 <Subspace, Double> o1, Tuple2 <Subspace, Double> o2) {
				return -o1.f1.compareTo(o2.f1);
			}
		});
		return result;
	}

	public List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>> listSubspaceByCol() {
		List <Tuple2 <String, List <Tuple2 <Subspace, Double>>>> list = new ArrayList <>();
		for (Entry <String, Integer> name_index : colNameMap.entrySet()) {
			List <Tuple2 <Subspace, Double>> result = new ArrayList <>();
			String colName = name_index.getKey();
			Map <Object, Double> valueMap = valueMaps[name_index.getValue()];
			for (Entry <Object, Double> value_impact : valueMap.entrySet()) {
				result.add(Tuple2.of(new Subspace(colName, value_impact.getKey()), value_impact.getValue()));
			}
			list.add(Tuple2.of(colName, result));
		}
		return list;
	}

	public List <Tuple2 <Subspace, Double>> listSingleShapeSubspace() {
		List <Tuple2 <Subspace, Double>> result = new ArrayList <>();

		for (Entry <String, Integer> name_index : colNameMap.entrySet()) {
			String colName = name_index.getKey();
			Map <Object, Double> valueMap = valueMaps[name_index.getValue()];
			for (Entry <Object, Double> value_impact : valueMap.entrySet()) {
				result.add(Tuple2.of(new Subspace(colName, value_impact.getKey()), value_impact.getValue()));
			}
		}

		Collections.sort(result, new Comparator <Tuple2 <Subspace, Double>>() {
			@Override
			public int compare(Tuple2 <Subspace, Double> o1, Tuple2 <Subspace, Double> o2) {
				return -o1.f1.compareTo(o2.f1);
			}
		});
		return result;
	}

	public List <Tuple3 <Subspace, Subspace, Double>> searchDoubleSubspace(LocalOperator <?> table) {
		List <Tuple3 <Subspace, Subspace, Double>> result = new ArrayList <>();

		List <Integer> possibleIndexes = new ArrayList <>();
		for (int i = 0; i < valueMaps.length; i++) {
			if (valueMaps[i].size() > 0) {
				possibleIndexes.add(i);
			}
		}
		final int totalCount = table.getOutputTable().getNumRow();
		final int minCount = (int) Math.round(totalCount * this.threshold);
		for (int i = 0; i < possibleIndexes.size(); i++) {
			String name1 = "`" + colNames[possibleIndexes.get(i)] + "`";
			for (int j = i + 1; j < possibleIndexes.size(); j++) {
				String name2 = "`" + colNames[possibleIndexes.get(j)] + "`";
				List <Row> rows = table
					.groupBy(name1 + "," + name2, name1 + "," + name2 + ", COUNT(" + name1 + ") AS cnt")
					.filter("cnt>=" + minCount)
					.getOutputTable()
					.getRows();

				for (Row row : rows) {
					result.add(Tuple3.of(
						new Subspace(name1, row.getField(0)),
						new Subspace(name2, row.getField(1)),
						((Number) row.getField(2)).doubleValue() / totalCount
					));
				}
			}
		}

		Collections.sort(result, new Comparator <Tuple3 <Subspace, Subspace, Double>>() {
			@Override
			public int compare(Tuple3 <Subspace, Subspace, Double> o1, Tuple3 <Subspace, Subspace, Double> o2) {
				return -o1.f2.compareTo(o2.f2);
			}
		});
		return result;
	}

	private static int[] getTimestampCols(TableSchema schema) {
		List <Integer> tsCols = new ArrayList <>();
		for (int i = 0; i < schema.getFieldNames().length; i++) {
			if (Types.SQL_TIMESTAMP == schema.getFieldType(i).get()) {
				tsCols.add(i);
			}
		}
		int[] tsColIndices = new int[tsCols.size()];
		for (int i = 0; i < tsColIndices.length; i++) {
			tsColIndices[i] = tsCols.get(i);
		}
		return tsColIndices;
	}

	static public class MutableInteger {
		private int value;

		public MutableInteger(int var1) {
			this.setValue(var1);
		}

		public int hashCode() {
			return this.getValue();
		}

		public boolean equals(Object var1) {
			return var1 instanceof MutableInteger
				&& ((MutableInteger) var1).getValue() == this.getValue();
		}

		public void setValue(int var1) {
			this.value = var1;
		}

		public int getValue() {
			return this.value;
		}
	}

}
