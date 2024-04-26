package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author weibo zhao
 */

public class DateDiff extends ScalarFunction {

	private static final long serialVersionUID = 6298088633116239045L;

	public Long eval(String end, String start) {
		if (start == null || end == null) {
			return null;
		}
		try {
			Timestamp tsEnd = Timestamp.valueOf(end);
			Timestamp tsStart = Timestamp.valueOf(start);
			return (tsEnd.getTime() - tsStart.getTime()) / 86400000L;
		} catch (Exception e) {
			return null;
		}
	}

	public Long eval(String end, Timestamp start) {
		if (start == null || end == null) {
			return null;
		}
		try {
			Timestamp tsEnd = Timestamp.valueOf(end);
			return (tsEnd.getTime() - start.getTime()) / 86400000L;
		} catch (Exception e) {
			return null;
		}
	}

	public Long eval(Timestamp end, Timestamp start) {
		if (start == null || end == null) {
			return null;
		}
		return (end.getTime() - start.getTime()) / 86400000L;
	}

	public Long eval(Timestamp end, String start) {
		if (start == null || end == null) {
			return null;
		}
		try {
			Timestamp tsStart = Timestamp.valueOf(start);
			return (end.getTime() - tsStart.getTime()) / 86400000L;
		} catch (Exception e) {
			return null;
		}
	}
}
