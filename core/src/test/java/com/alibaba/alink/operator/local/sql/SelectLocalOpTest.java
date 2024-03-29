package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.BatchSqlOperators;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectLocalOpTest {
	@Test
	public void testSelectLocalOp() {
		//String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		//String SCHEMA_STR
		//	= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		//LocalOperator <?> data = new TableSourceLocalOp(
		//	new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).collectMTable());
		LocalOperator <?> data = IrisData.getLocalSourceOp();
		data.link(new SelectLocalOp().setClause("category as a")).print();
	}


	@Test
	public void testSimpleSelect() throws Exception {
		data().link(
			new SelectLocalOp()
				.setClause("f_double, f_long")
		).print();
	}

	@Test
	public void testSimpleSelect2() throws Exception {
		data().select("f_double, f_long").print();
	}

	//@Test
	//public void testSelect() throws Exception {
	//	data().link(
	//		new SelectLocalOp()
	//			.setClause("f_double, `f_l.*`")
	//	).print();
	//}
	//
	//@Test
	//public void testSelect2() throws Exception {
	//	data().select("f_double, `f_l.*`").print();
	//}
	//
	//@Test
	//public void testSelect3() throws Exception {
	//	data().link(
	//		new SelectLocalOp()
	//			.setClause("`f_d.*`, `f_l.*`")
	//	).print();
	//}

	@Test
	public void testCSelect() throws Exception {
		data().link(
			new SelectLocalOp()
				.setClause("f_double, `f_l.*`, f_double+1 as f_double_1")
		).print();
	}

	@Test
	public void testCSelect2() throws Exception {
		data().select("f_double, `f_l.*`,f_double+1 as f_double_1").print();
	}

	@Test
	public void testCSelect3() throws Exception {
		data().select("f_double as fr, *, f_long As fr2").print();
	}

	@Test
	public void testCSelect4() throws Exception {
		data()
			.select("f_string as fas, f_double, f_long")
			.select("fas, f_double")
			.select("fas as as2, f_double as fd, f_double")
			.print();
	}

	@Test
	public void testCSelect5() throws Exception {
		String[] originSqlCols = data().select("f_string, f_double, f_string, f_string").getColNames();
		String[] simpleSelectCols = data().select("f_string, f_double, f_string, f_string").getColNames();
		Assert.assertArrayEquals(originSqlCols, simpleSelectCols);
	}

	private LocalOperator <?> data() {
		List <Row> testArray = Arrays.asList(
			Row.of("a", 1L, 1, 2.0, true),
			Row.of(null, 2L, 2, -3.0, true),
			Row.of("c", null, null, 2.0, false),
			Row.of("a", 0L, 0, null, null)
		);

		String[] colNames = new String[] {"f_string", "f_long", "f_lint", "f_double", "f_boolean"};

		return new MemSourceLocalOp(testArray, colNames);
	}

}