package com.alibaba.alink.common.sql.builtin.string.string;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将srcStr（源字符串）中parttern 匹配的字符串替换为 replaceStr。
 *
 * @author weibo zhao
 */
public class RegExpReplace extends ScalarFunction {

	private static final long serialVersionUID = 6928900303963924422L;

	public String eval(String srcStr, String pattern, String replaceStr) {
		if (srcStr == null || pattern == null || replaceStr == null) {
			return null;
		}
		// 创建 Pattern 对象
		Pattern r = Pattern.compile(pattern);
		// 现在创建 matcher 对象
		Matcher m = r.matcher(srcStr);

		return m.replaceAll(replaceStr);
	}
}
