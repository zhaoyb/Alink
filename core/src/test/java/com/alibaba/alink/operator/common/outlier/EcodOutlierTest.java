package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.outlier.EcodOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.EcodOutlierStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class EcodOutlierTest extends AlinkTestBase {
	String testCsv2D = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/contamination.csv";
	String schemaStr2D = "id int, f0 double, f1 double, label string";
	List <Row> testDf = Arrays.asList(
		Row.of(new Timestamp(117, 11, 1, 0, 0, 0, 0), 0.0, 7.0),
		Row.of(new Timestamp(117, 11, 2, 0, 0, 0, 0), 1.0, 6.0),
		Row.of(new Timestamp(117, 11, 3, 0, 0, 0, 0), 1.0, 6.0),
		Row.of(new Timestamp(117, 11, 4, 0, 0, 0, 0), 2.0, 5.0),
		Row.of(new Timestamp(117, 11, 5, 0, 0, 0, 0), 2.0, 5.0),
		Row.of(new Timestamp(117, 11, 6, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 7, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 8, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 9, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 10, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 11, 0, 0, 0, 0), 3.0, 4.0),
		Row.of(new Timestamp(117, 11, 12, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 13, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 14, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 15, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 16, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 17, 0, 0, 0, 0), 4.0, 3.0),
		Row.of(new Timestamp(117, 11, 18, 0, 0, 0, 0), 5.0, 2.0),
		Row.of(new Timestamp(117, 11, 19, 0, 0, 0, 0), 5.0, 2.0),
		Row.of(new Timestamp(117, 11, 20, 0, 0, 0, 0), 6.0, 1.0),
		Row.of(new Timestamp(117, 11, 21, 0, 0, 0, 0), 6.0, 1.0),
		Row.of(new Timestamp(117, 11, 22, 0, 0, 0, 0), 7.0, 0.0)
	);

	@Test
	public void testBatchOp() throws Exception {
		String[] genCols = {"f0", "f1"};

		for (int k = 0; k < 1; k++) {
			long envId = MLEnvironmentFactory.getNewMLEnvironmentId();
			MLEnvironmentFactory.get(envId).getExecutionEnvironment().setParallelism(1);
			EvalOutlierBatchOp copodOutlier = new CsvSourceBatchOp()
				.setFilePath(testCsv2D)
				.setSchemaStr(schemaStr2D)
				.setMLEnvironmentId(envId)
				.link(
					new EcodOutlierBatchOp()
						.setFeatureCols(genCols)
						.setPredictionCol("outlier")
						.setPredictionDetailCol("details")
						.setMLEnvironmentId(envId)
				)
				.select("label, details")
				.link(
					new EvalOutlierBatchOp()
						.setLabelCol("label")
						.setOutlierValueStrings("1")
						.setPredictionDetailCol("details")
						.setMLEnvironmentId(envId)
				);

			OutlierMetrics metrics = copodOutlier.collectMetrics();
			double auc = metrics.getAuc();
			System.out.println(auc);

			MLEnvironmentFactory.remove(envId);
		}

	}

	@Test
	public void testBatchOp2() throws Exception {
		String[] genCols = {"f0", "f1"};

		BatchOperator.setParallelism(1);

		for (int k = 0; k < 1; k++) {
			EvalOutlierBatchOp copodOutlier =
				new CsvSourceBatchOp()
					.setFilePath(testCsv2D)
					.setSchemaStr(schemaStr2D)
					.link(
						new EcodOutlierBatchOp()
							.setFeatureCols(genCols)
							.setPredictionCol("outlier")
							.setPredictionDetailCol("details")
					)
					.select("label, details")
					.link(
						new EvalOutlierBatchOp()
							.setLabelCol("label")
							.setOutlierValueStrings("1")
							.setPredictionDetailCol("details")
					);

			OutlierMetrics metrics = copodOutlier.collectMetrics();
			double auc = metrics.getAuc();
			System.out.println(auc);
			//Assert.assertEquals(auc,0.998,0.001);
		}

	}

	@Test
	public void testStreamOp() throws Exception {
		String[] schema = new String[] {"ts", "f0", "f1"};
		StreamOperator sourceData = new MemSourceStreamOp(testDf, schema);

		EcodOutlierStreamOp detector = new EcodOutlierStreamOp()
			.setFeatureCols(new String[] {"f0", "f1"})
			.setTimeCol("ts")
			.setPredictionCol("outlier")
			.setPredictionDetailCol("score");

		detector.linkFrom(sourceData);
		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(detector);
		StreamOperator.execute();
		List <Row> results = sink.getAndRemoveValues();
		Assert.assertEquals(results.size(), testDf.size());
	}
}