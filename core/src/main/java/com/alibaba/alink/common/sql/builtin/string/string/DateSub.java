package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author weibo zhao
 */
public class DateSub extends ScalarFunction {
	private static final long serialVersionUID = -8304706070267129499L;

	public String eval(String end, int days) {
		if (end == null) {
			return null;
		}
		try {
			Timestamp tsEnd = Timestamp.valueOf(end);
			long ld = (tsEnd.getTime() - days * 86400000L);
			return new Timestamp(ld).toString();
		} catch (Exception e) {
			return null;
		}
	}

	public String eval(Timestamp end, int days) {
		if (end == null) {
			return null;
		}
		long ld = (end.getTime() - days * 86400000L);
		return new Timestamp(ld).toString();

	}
}
