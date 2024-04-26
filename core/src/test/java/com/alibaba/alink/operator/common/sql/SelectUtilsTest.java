package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class SelectUtilsTest extends AlinkTestBase {
	@Test
	public void testSelectRegexUtil() {
		String[] colNames = new String[] {"sepal", "petal", "sepal_width", "petal_length", "category"};
		String clause1 = "`(petal|sepal)?`, `a`";
		String clause2 = "`a`, `(petal|sepal)?`";
		String clause3 = "`dwd_.*`, `(petal|sepal)?`";
		String clause4 = "`(petal|sepal)?+.+`";
		String clause5 = "`(petal|sepal)?+.+`, `category` as `label`";
		String s1 = "`sepal`,`petal`";
		String s2 = " `sepal`,`petal`";
		String s3 = "`sepal_width`,`petal_length`,`category`";
		String s4 = "`sepal_width`,`petal_length`,`category`, `category` as `label`";
		Assert.assertEquals(s1, SelectUtils.convertRegexClause2ColNames(colNames, clause1));
		Assert.assertEquals(s2, SelectUtils.convertRegexClause2ColNames(colNames, clause2));
		Assert.assertEquals(s2, SelectUtils.convertRegexClause2ColNames(colNames, clause3));
		Assert.assertEquals(s3, SelectUtils.convertRegexClause2ColNames(colNames, clause4));
		Assert.assertEquals(s4, SelectUtils.convertRegexClause2ColNames(colNames, clause5));
	}

	@Test
	public void testIsSimpleClause() {
		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		Assert.assertTrue(SelectUtils.isSimpleSelect("f_long, f_double", colNames));
		Assert.assertFalse(SelectUtils.isSimpleSelect("f_long+1 as f1, f_double", colNames));
		Assert.assertTrue(SelectUtils.isSimpleSelect("*", colNames));
		Assert.assertTrue(SelectUtils.isSimpleSelect("*, f_double as fr_1", colNames));
	}

	@Test
	public void testSplit() {
		String sqlStr = "ts, TIMESTAMPDIFF(DAY, ts, TIMESTAMP '2022-05-11 00:00:00') AS past_days,"
			+ " TIMESTAMPDIFF(WEEK, ts, TIMESTAMP '2022-05-11 00:00:00') AS past_weeks,"
			+ " TIMESTAMPDIFF(MONTH, ts, TIMESTAMP '2022-05-11 00:00:00') AS past_months,"
			+ " TIMESTAMPDIFF(YEAR, ts, TIMESTAMP '2022-05-11 00:00:00') AS past_years";

		String[] colNames = new String[] {"user_id", "ts", "type"};

		Tuple2 <String, Boolean>[] t2 = SelectUtils.splitClauseBySimpleClause(sqlStr, colNames);
		System.out.println(JsonConverter.toJson(t2));
	}

}

