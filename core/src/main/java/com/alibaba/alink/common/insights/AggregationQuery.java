package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.sql.GroupByLocalOp;
import com.alibaba.alink.params.statistics.HasIsSingleThread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.insights.Mining.MEASURE_NAME_PREFIX;

public class AggregationQuery {

	public static List <LocalOperator <?>> query(LocalOperator <?> subData,
												 Breakdown breakdown,
												 List <Measure> measures,
												 int threadNum) {
		String breakdownCol = breakdown.colName;
		List <LocalOperator <?>> result = new ArrayList <>();

		LocalOperator <?> dataQuery;
		String groupByClause = "`" + breakdownCol + "`";
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr.udfName()).append("(`").append(measure.colName).append("`) AS ")
				.append(
					MEASURE_NAME_PREFIX).append(i);
		}

		LocalOperator <?> groupByOp = new GroupByLocalOp(groupByClause, sbdAggr.toString());
		groupByOp.set(HasIsSingleThread.THREAD_NUM, threadNum);

		dataQuery = subData.link(groupByOp);

		if (measures.size() == 1) {
			result.add(dataQuery);
		} else {
			String[] parts = TableUtil.schema2SchemaStr(dataQuery.getSchema()).split(",");
			MTable mt = dataQuery.getOutputTable();
			for (int i = 1; i <= measures.size(); i++) {
				int[] colIndexes = new int[] {0, i};
				ArrayList <Row> rows = new ArrayList <>();
				for (Row r : mt.getRows()) {
					rows.add(Row.project(r, colIndexes));
				}

				result.add(new MemSourceLocalOp(
					new MTable(rows, parts[0] + ", " + MEASURE_NAME_PREFIX + "0 " + parts[i].trim().split(" ")[1])));
			}
		}
		return result;
	}

	public static LocalOperator <?> query(LocalOperator <?> source,
										  List <Subspace> subspaces,
										  String expandingColName,
										  List <Object> expandingValues,
										  Breakdown breakdown,
										  List <Measure> measures) {
		if (subspaces.size() > 0) {
			StringBuilder sbd = new StringBuilder();
			sbd.append(createFilterSubspaceClause(subspaces.get(0)));
			for (int i = 1; i < subspaces.size(); i++) {
				sbd.append(" AND ").append(createFilterSubspaceClause(subspaces.get(i)));
			}
			if (null != expandingColName && null != expandingValues && expandingValues.size() > 0) {
				sbd.append(" AND ").append(createFilterExpandingClause(expandingColName, expandingValues));
			}

			source = source.filter(sbd.toString());
		}

		String groupByClause = "`" + breakdown.colName + "`";
		if (null != expandingColName) {
			groupByClause += ", `" + expandingColName + "`";
		}
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr.udfName()).append("(`").append(measure.colName).append("`) AS ")
				.append(
					MEASURE_NAME_PREFIX).append(i);
		}

		//System.out.println("group sql: " + sbdAggr);
		LocalOperator <?> dataAggr = source.groupBy(groupByClause, sbdAggr.toString());
		return dataAggr;
	}

	public static List <LocalOperator <?>> sameSubspaceColQuery(LocalOperator <?> source,
																String subspaceCol,
																List <Subspace> subspaces,
																Breakdown breakdown,
																List <Measure> measures,
																int threadNum) {
		String groupByClause = "`" + subspaceCol + "`" + ", `" + breakdown.colName + "`";
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr.udfName()).append("(`").append(measure.colName).append("`) AS ")
				.append(
					MEASURE_NAME_PREFIX).append(i);
		}

		LocalOperator <?> groupOp = new GroupByLocalOp(groupByClause, sbdAggr.toString());
		groupOp.getParams().set(HasIsSingleThread.THREAD_NUM, threadNum);

		LocalOperator <?> dataQuery = source.link(groupOp);
		List <LocalOperator <?>> result = new ArrayList <>();
		List <List <Row>> list = new ArrayList <>();
		Map <Object, Integer> indexMap = new HashMap <>();
		for (int i = 0; i < subspaces.size(); i++) {
			List <Row> rows = new ArrayList <>();
			list.add(rows);
			indexMap.put(subspaces.get(i).value, i);
		}

		if (measures.size() == 1) {
			result.add(dataQuery);
		} else {
			MTable mt = dataQuery.getOutputTable();
			for (Row r : mt.getRows()) {
				Object value = r.getField(0);
				if (!indexMap.containsKey(value)) {
					continue;
				}
				int idx = indexMap.get(value);
				list.get(idx).add(r);
			}
			for (int i = 0; i < list.size(); i++) {
				result.add(new MemSourceLocalOp(list.get(i), mt.getSchemaStr()));
			}
		}
		return result;
	}

	public static List <LocalOperator <?>> sameSubspaceColQuery2(LocalOperator <?> source,
																 String subspaceCol,
																 List <Subspace> subspaces,
																 Breakdown breakdown,
																 List <Measure> measures,
																 int threadNum) {
		String groupByClause = "`" + subspaceCol + "`" + ", `" + breakdown.colName + "`";
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr.udfName()).append("(`").append(measure.colName).append("`) AS ")
				.append(
					MEASURE_NAME_PREFIX).append(i);
		}

		LocalOperator <?> groupOp = new GroupByLocalOp(groupByClause, sbdAggr.toString());
		groupOp.getParams().set(HasIsSingleThread.THREAD_NUM, threadNum);

		LocalOperator <?> dataQuery = source.link(groupOp);
		List <LocalOperator <?>> result = new ArrayList <>();
		List <List <Row>> list = new ArrayList <>();
		Map <Object, Integer> indexMap = new HashMap <>();
		for (int i = 0; i < subspaces.size(); i++) {
			List <Row> rows = new ArrayList <>();
			list.add(rows);
			indexMap.put(subspaces.get(i).value, i);
		}

		MTable mt = dataQuery.getOutputTable();
		for (Row r : mt.getRows()) {
			Object value = r.getField(0);
			if (!indexMap.containsKey(value)) {
				continue;
			}
			int idx = indexMap.get(value);
			list.get(idx).add(r);
		}
		for (int i = 0; i < list.size(); i++) {
			result.add(new MemSourceLocalOp(list.get(i), mt.getSchemaStr()));
		}

		return result;
	}

	private static String createFilterSubspaceClause(Subspace subspace) {
		if (subspace.value instanceof String) {
			return subspace.colName + "='" + subspace.value + "'";
		} else {
			return subspace.colName + "=" + subspace.value;
		}
	}

	private static String createFilterExpandingClause(String expandingColName, List <Object> expandingValues) {
		StringBuilder sbd = new StringBuilder();
		sbd.append("`").append(expandingColName).append("` IN (");
		if (expandingValues.get(0) instanceof String) {
			sbd.append("'").append(expandingValues.get(0)).append("'");
			for (int i = 1; i < expandingValues.size(); i++) {
				sbd.append(", '").append(expandingValues.get(i)).append("'");
			}
		} else {
			sbd.append(expandingValues.get(0));
			for (int i = 1; i < expandingValues.size(); i++) {
				sbd.append(", ").append(expandingValues.get(i));
			}
		}
		sbd.append(")");
		return sbd.toString();
	}

}
