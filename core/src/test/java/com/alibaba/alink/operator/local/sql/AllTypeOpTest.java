package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

public class AllTypeOpTest {
	// for 1.9, local sql for timestamp has something wrong, only support in 1.13.

	@Test
	public void test1() {
		int nRows = 5;

		Tuple2 <MTable, String[]> t2 = createData(nRows, 123);
		List <Tuple2 <String, LocalOperator <?>>> ops = createOps(t2.f1);

		MemSourceLocalOp data = new MemSourceLocalOp(t2.f0);
		data.print();

		for (Tuple2 <String, LocalOperator <?>> op : ops) {
			System.out.println("\n>>> " + op.f0);
			try {
				data.link(op.f1).print();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		String[] colNames = t2.f1;
		String[] newColNames = new String[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			newColNames[i] = colNames[i] + "_2";
		}
		LocalOperator <?> data1 = data.as(newColNames);

		String join_select_clause = getJoinSelectClause(colNames, newColNames);

		for (int i = 0; i < colNames.length; i++) {
			System.out.println("\n>>> " + "join on " + colNames[i]);
			new JoinLocalOp()
				.setJoinPredicate(colNames[i] + "=" + newColNames[i])
				.linkFrom(data, data1)
				.print();
			System.out.println("\n>>> " + "left join on " + colNames[i]);
			new LeftOuterJoinLocalOp()
				.setSelectClause(join_select_clause)
				.setJoinPredicate(colNames[i] + "=" + newColNames[i])
				.linkFrom(data, data1)
				.print();
			System.out.println("\n>>> " + "right join on " + colNames[i]);
			new RightOuterJoinLocalOp()
				.setSelectClause(join_select_clause)
				.setJoinPredicate(colNames[i] + "=" + newColNames[i])
				.linkFrom(data, data1)
				.print();
		}

		//LocalOperator <?> data2 = new UnionAllLocalOp().linkFrom(data, data);
		//LocalOperator <?> data3 = new UnionAllLocalOp().linkFrom(data2, data);
		//Assert.assertEquals(2 * nRows, data2.getOutputTable().getNumRow());
		//Assert.assertEquals(nRows, new UnionLocalOp().linkFrom(data, data).getOutputTable().getNumRow());
		//
		//Assert.assertEquals(2 * nRows, new IntersectAllLocalOp().linkFrom(data3, data2).getOutputTable().getNumRow());
		//Assert.assertEquals(nRows, new IntersectLocalOp().linkFrom(data3, data2).getOutputTable().getNumRow());
		//
		//Assert.assertEquals(2 * nRows, new MinusAllLocalOp().linkFrom(data3, data).getOutputTable().getNumRow());
		//Assert.assertEquals(0, new MinusLocalOp().linkFrom(data3, data).getOutputTable().getNumRow());

	}

	private String getJoinSelectClause(String[] colNames, String[] newColNames) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {sbd.append(",");}
			sbd.append(colNames[i]);
		}
		for (int i = 0; i < newColNames.length; i++) {
			sbd.append(",").append(newColNames[i]);
		}
		return sbd.toString();
	}

	private Tuple2 <MTable, String[]> createData(int nRows, int seed) {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		Tuple3 <String, String, Object[]>[] array = new Tuple3[] {
			Tuple3.of("f_string", "string", new String[] {"alpha", "beta", "gamma"}),
			Tuple3.of("f_int", "int", new Integer[] {1, 2, 3, 4, 5}),
			Tuple3.of("f_long", "long", new Long[] {1L, 2L, 3L, 4L}),
			Tuple3.of("f_float", "float", new Float[] {0.1f, 0.2f, 0.3f}),
			Tuple3.of("f_double", "double", new Double[] {0.1, 0.2, 0.3}),
			Tuple3.of("f_boolean", "boolean", new Boolean[] {true, false}),
			Tuple3.of("f_timestamp", "timestamp", new Timestamp[] {new Timestamp(1000), new Timestamp(3000)}),
		};

		Random random = new Random(seed);
		int nCols = array.length;
		List <Row> rows = new ArrayList <>();
		for (int k = 0; k < nRows; k++) {
			Row row = new Row(nCols);
			for (int i = 0; i < nCols; i++) {
				row.setField(i, array[i].f2[random.nextInt(array[i].f2.length)]);
			}
			rows.add(row);
		}

		String[] colNames = new String[nCols];
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < nCols; i++) {
			colNames[i] = array[i].f0;
			if (i > 0) {sbd.append(",");}
			sbd.append(array[i].f0).append(" ").append(array[i].f1);
		}

		return Tuple2.of(new MTable(rows, sbd.toString()), colNames);
	}

	private List <Tuple2 <String, LocalOperator <?>>> createOps(String[] colNames) {
		List <Tuple2 <String, LocalOperator <?>>> ops = new ArrayList <>();

		String clause_all_cols = String.join(",", colNames);

		ops.add(Tuple2.of("select all",
			new SelectLocalOp()
				.setClause(clause_all_cols)
		));

		ops.add(Tuple2.of("distinct",
			new DistinctLocalOp()
		));

		for (int i = 0; i < colNames.length; i++) {
			ops.add(Tuple2.of("order by " + colNames[i],
				new OrderByLocalOp()
					.setClause(colNames[i])
					.setLimit(5)
			));
		}

		for (int i = 0; i < colNames.length; i++) {
			ops.add(Tuple2.of("group by " + colNames[i],
				new GroupByLocalOp()
					.setGroupByPredicate(colNames[i])
					.setSelectClause(colNames[i] + ", COUNT(*) AS cnt")
			));
		}

		for (int i = 0; i < colNames.length; i++) {
			ops.add(Tuple2.of("select " + colNames[i],
				new SelectLocalOp()
					.setClause(colNames[i])
			));
		}

		return ops;
	}
}
