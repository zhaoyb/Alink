package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 判断 srcStr（源字符串）中 是否存在 parttern 匹配的字符串。
 *
 * @author weibo zhao
 */
public class RegExp extends ScalarFunction {

	private static final long serialVersionUID = -194627833036515975L;

	public boolean eval(String srcStr, String pattern) {
		if (srcStr == null || pattern == null) {
			return srcStr == null && pattern == null;
		}
		// 创建 Pattern 对象
		Pattern r = Pattern.compile(pattern);
		// 现在创建 matcher 对象
		Matcher m = r.matcher(srcStr);

		return m.find();
	}
}
