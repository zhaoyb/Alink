package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author weibo zhao
 */
public class DateAdd extends ScalarFunction {

	private static final long serialVersionUID = -4716626352353627712L;

	public String eval(String end, int days) {
		if (end == null) {
			return null;
		}
		try {
			Timestamp tsEnd = Timestamp.valueOf(end);
			long ld = (tsEnd.getTime() + days * 86400000L);
			return new Timestamp(ld).toString();
		} catch (Exception e) {
			return null;
		}
	}

	public String eval(Timestamp end, int days) {
		if (end == null) {
			return null;
		}
		long ld = (end.getTime() + days * 86400000L);
		return new Timestamp(ld).toString();

	}
}
