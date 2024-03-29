package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class BaseSqlApiLocalOpTest {

	Row[] rows = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
	};

	Row[] rows1 = new Row[] {
		Row.of("1L", "1L", 15.0),
		Row.of("2L", "3L", 12.0),
		Row.of("4L", "3L", 10.0),
	};

	@Test
	public void test1() throws Exception {
		LocalOperator data = new MemSourceLocalOp(rows, new String[] {"f1", "f2", "f3"});
		LocalOperator data1 = new MemSourceLocalOp(rows1, new String[] {"f1", "f2", "f3"});

		new FullOuterJoinLocalOp().setJoinPredicate("a.f1=b.f1")
			.setSelectClause("case when a.f1 is null then b.f1 when b.f1 is null then a.f1 else b.f1 end as uid, "
				+ "case when a.f1 is null then b.f3 when b.f1 is null then a.f3 else b.f3 end as factors")
			.linkFrom(data, data1).print();
	}

	@Test
	public void test() {
		LocalOperator data = new MemSourceLocalOp(rows, new String[] {"f1", "f2", "f3"});
		Assert.assertEquals(data.select("f1").getColNames().length, 1);
		Assert.assertEquals(data.select(new String[] {"f1", "f2"}).getColNames().length, 2);
		Assert.assertEquals(new JoinLocalOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new LeftOuterJoinLocalOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new RightOuterJoinLocalOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(new FullOuterJoinLocalOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1 as f1")
			.linkFrom(data, data).getColNames().length, 1);
		Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().minus(data,data).getColNames().length,3);
		//Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().minusAll(data,data).getColNames().length,3);
		Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().union(data,data).getColNames().length,3);
		Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().unionAll(data,data).getColNames().length,3);
		Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().intersect(data,data).getColNames().length,3);
		//Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().intersectAll(data,data).getColNames().length,3);
		Assert.assertEquals(LocalMLEnvironment.getInstance().getSqlExecutor().distinct(data).getColNames().length,3);
		////Assert.assertEquals(new MinusLocalOp().linkFrom(data, data).getColNames().length, 3);
		//////Assert.assertEquals(new MinusAllLocalOp().linkFrom(data, data).getColNames().length, 3);
		////Assert.assertEquals(new UnionLocalOp().linkFrom(data, data).getColNames().length, 3);
		////Assert.assertEquals(new UnionAllLocalOp().linkFrom(data, data).getColNames().length, 3);
		////Assert.assertEquals(new IntersectLocalOp().linkFrom(data, data).getColNames().length, 3);
		//////Assert.assertEquals(new IntersectAllLocalOp().linkFrom(data, data).getColNames().length, 3);
		//Assert.assertEquals(new DistinctLocalOp().linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new WhereLocalOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new FilterLocalOp().setClause("f1='1L'").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new GroupByLocalOp().setGroupByPredicate("f1").setSelectClause("f1, sum(f3)")
			.linkFrom(data).getColNames().length, 2);
		Assert.assertEquals(new AsLocalOp().setClause("ff1,ff2,ff3").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(new OrderByLocalOp().setClause("f1").linkFrom(data).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 2).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 2, true).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 0, 1).getColNames().length, 3);
		Assert.assertEquals(data.orderBy("f1", 0, 1, false).getColNames().length, 3);
	}


}