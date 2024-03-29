package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.commons.lang3.StringUtils;

public class OrderUtils {

	public static String getColsAndOrdersStr(String clause, TableSchema schema,
											 boolean isAscending) {

		Tuple2 <String[], boolean[]> t2 = getColsAndOrders(clause, schema, isAscending);
		String[] selectCols = t2.f0;
		boolean[] orders = t2.f1;
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < selectCols.length; i++) {
			sbd.append(String.format("`%s` %s", selectCols[i], orders[i] ? "asc" : "desc"));
			if (i != selectCols.length - 1) {
				sbd.append(", ");
			}
		}
		return sbd.toString();
	}

	public static Tuple2 <String[], boolean[]> getColsAndOrders(String clause, TableSchema schema,
																boolean isAscending) {

		if (clause == null || clause.trim().isEmpty()) {
			throw new AkIllegalArgumentException("Clause must be set. It's format is col1 desc, col2, col3 asc");
		}

		String[] splits = StringUtils.split(clause.trim(), ",");
		int len = splits.length;
		String[] selectCols = new String[len];
		boolean[] orders = new boolean[len];

		for (int i = 0; i < len; i++) {
			String[] t = StringUtils.split(splits[i].trim(), " ");
			if (t.length == 1) {
				selectCols[i] = checkCol(t[0], schema);
				orders[i] = isAscending;
			} else if (t.length == 2) {
				selectCols[i] = checkCol(t[0], schema);
				if (t[1].trim().equalsIgnoreCase("asc") ||
					t[1].trim().equalsIgnoreCase("desc")) {
					orders[i] = t[1].trim().equalsIgnoreCase("asc");
				} else {
					throw new AkIllegalArgumentException("Order must be asc or desc.");
				}
			} else {
				throw new AkIllegalArgumentException("Clause format is col1 desc, col2, col3 asc.");
			}
		}
		return Tuple2.of(selectCols, orders);
	}

	private static String checkCol(String col, TableSchema schema) {
		col = col.trim().replace("`", "");
		if (schema != null) {
			TableUtil.findColIndexWithAssert(schema, col);
		}
		return col;
	}
}
