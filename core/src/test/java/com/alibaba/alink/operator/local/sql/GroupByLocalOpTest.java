package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GroupByLocalOpTest {

	@Test
	public void test() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> batch_data = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));
		LocalOperator <?> op = new GroupByLocalOp()
			.setGroupByPredicate("f1")
			//.setSelectClause("f1, count(*) as cnt, avg(f2) as f2, f1 as f1_bak, mtable_agg(f2, f3) as c3, avg(f2)"
			//	+ ", max_batch(f2), min_batch(f3)")
			.setSelectClause("f1, max_batch(f2), min_batch(f3)");
		batch_data = batch_data.link(op);
		batch_data.print();
	}

	@Test
	public void testAll() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> batch_data = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));
		batch_data.groupBy("f1", "f1, max(f2) as c1, min(f3) as c2")
			.print();

		batch_data.groupBy("1", "max(f2) as c1, min(f3) as c2")
			.print();

		batch_data.groupBy("max(f2) as c1, min(f3) as c2")
			.print();

	}

	@Test
	public void testGroupByLocalOp() {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		LocalOperator <?> batch_data = new TableSourceLocalOp(new MTable(df, "f1 string, f2 int, f3 double"));
		LocalOperator <?> op = new GroupByLocalOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2");
		batch_data = batch_data.link(op);
		batch_data.print();
	}

}