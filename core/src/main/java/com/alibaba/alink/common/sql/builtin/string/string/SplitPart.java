package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

/**
 * @author dota.zk
 * @reference http://odps.alibaba-inc.com/doc.htm SQL->udf->split_part
 * @date 23/05/2018
 */
public class SplitPart extends ScalarFunction {

	private static final long serialVersionUID = 3053182766744474677L;

	public String eval(String src, String delimiter, Integer nth) {
		if (src == null || delimiter == null || nth == null) { return null; }
		if (nth < 1) { return ""; }
		final String[] L = StringUtils.splitByWholeSeparatorPreserveAllTokens(src, delimiter);
		if (L.length < nth) {
			return "";
		}
		return L[nth - 1];
	}

	public String eval(String src, String delimiter, Integer start, Integer end) {
		if (src == null || delimiter == null || start == null || end == null) { return null; }
		if (delimiter.isEmpty()) { return src; }
		if (start > end) { return ""; }
		if (start < 0) { start = 1; }
		final String[] L = StringUtils.splitByWholeSeparatorPreserveAllTokens(src, delimiter);
		if (end > L.length) { end = L.length; }
		if (start > end) { return ""; }
		return StringUtils.join(L, delimiter, start - 1, end);
	}
}
