package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

/**
 * 将srcStr（源字符串）按delimiter1分成“key-value”对，按delimiter2将key-value对分开，返回“key”所对应的value。
 *
 * @author dota.zk
 * @date 23/05/2018
 */
public class KeyValue extends ScalarFunction {

	private static final long serialVersionUID = 772339384889917291L;

	public String eval(String src, String delimiter1, String delimiter2, String key) {
		if (src == null || delimiter1 == null || delimiter2 == null || key == null) {
			return null;
		}
		for (final String s : StringUtils.splitByWholeSeparator(src, delimiter1)) {
			final String[] L = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, delimiter2, 2);
			if (L[0].equals(key)) {
				return L.length == 2 ? L[1] : "";
			}
		}
		return null;
	}
}
