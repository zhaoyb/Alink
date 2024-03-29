//package com.alibaba.alink.operator.local.statistics;
//
//import org.apache.flink.types.Row;
//
//import com.alibaba.alink.operator.batch.dataproc.format.ColumnsToTripleBatchOp;
//import com.alibaba.alink.operator.batch.feature.OverWindowBatchOp;
//import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
//import junit.framework.TestCase;
//import org.junit.Test;
//
//import java.sql.Timestamp;
//import java.util.Arrays;
//
//public class SummarizerLocalOpTest extends TestCase {
//
//	@Test
//	public void test2() throws Exception {
//		Row[] testArray =
//			new Row[] {
//				Row.of("a", 1, 1.1, 1.2),
//				Row.of("b", null, 0.9, 1.0),
//				Row.of("c", 100, -0.01, 1.0),
//				Row.of("d", -99, 100.9, 0.1),
//				Row.of("a", 1, 1.1, 1.2),
//				Row.of("b", null, 0.9, 1.0),
//				Row.of("c", null, -0.01, 0.2),
//				Row.of("d", -99, 100.9, 0.3)
//			};
//
//		String[] colNames = new String[] {"col1", "col2", "col3", "col4"};
//
//		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);
//
//		source
//			.link(
//				new ColumnsToTripleBatchOp()
//					.setSelectedCols(colNames)
//					.setTripleColumnValueSchemaStr("colName string, val string")
//			)
//			.groupBy("colName", "colName, COUNT(DISTINCT val) AS valCount")
//			.lazyPrint(100);
//
//		source
//			.link(
//				new ColumnsToTripleBatchOp()
//					.setSelectedCols(colNames)
//					.setTripleColumnValueSchemaStr("colName string, val string")
//			)
//			.groupBy("colName, val", "colName, val, COUNT(val) AS cnt")
//			.link(
//				new OverWindowBatchOp()
//					.setOrderBy("cnt desc")
//					.setGroupCols("colName")
//					.setClause("ROW_NUMBER(cnt) AS rnk")
//					.setReservedCols("colName","val", "cnt")
//			)
//			.filter("rnk<=2")
//			.print(100);
//	}
//
//	@Test
//	public void test3() throws Exception {
//		String[] colNames = new String[] {"id", "user", "sell_time", "price"};
//
//		MemSourceBatchOp source = new MemSourceBatchOp(
//			new Row[] {
//				Row.of(1, "user2", Timestamp.valueOf("2021-01-01 00:01:00"), 20),
//				Row.of(2, "user1", Timestamp.valueOf("2021-01-01 00:02:00"), 50),
//				Row.of(3, "user2", Timestamp.valueOf("2021-01-01 00:03:00"), 30),
//				Row.of(4, "user1", Timestamp.valueOf("2021-01-01 00:06:00"), 60),
//				Row.of(5, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 40),
//				Row.of(6, "user2", Timestamp.valueOf("2021-01-01 00:06:00"), 20),
//				Row.of(7, "user2", Timestamp.valueOf("2021-01-01 00:07:00"), 70),
//				Row.of(8, "user1", Timestamp.valueOf("2021-01-01 00:08:00"), 80),
//				Row.of(9, "user1", Timestamp.valueOf("2021-01-01 00:09:00"), 40),
//				Row.of(10, "user1", Timestamp.valueOf("2021-01-01 00:10:00"), 20),
//				Row.of(11, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 30),
//				Row.of(12, "user1", Timestamp.valueOf("2021-01-01 00:11:00"), 50)
//			},
//			colNames
//		);
//
//		source
//			.link(
//				new ColumnsToTripleBatchOp()
//					.setSelectedCols(colNames)
//					.setTripleColumnValueSchemaStr("colName string, val string")
//			)
//			.groupBy("colName", "colName, COUNT(DISTINCT val) AS valCount")
//			.lazyPrint(100);
//
//		source
//			.link(
//				new ColumnsToTripleBatchOp()
//					.setSelectedCols(colNames)
//					.setTripleColumnValueSchemaStr("colName string, val string")
//			)
//			.groupBy("colName, val", "colName, val, COUNT(val) AS cnt")
//			.link(
//				new OverWindowBatchOp()
//					.setOrderBy("cnt desc")
//					.setGroupCols("colName")
//					.setClause("ROW_NUMBER(cnt) AS rnk")
//					.setReservedCols("colName","val", "cnt")
//			)
//			.filter("rnk<=2")
//			.print(100);
//
//	}
//}