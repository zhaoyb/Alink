package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class DvInsightDescriptionTest extends AlinkTestBase {

	@Test
	public void testOutstandingNo1() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "a1", 0));
		rows.add(Row.of(1, "a2", 0));
		rows.add(Row.of(3, "a3", 0));
		rows.add(Row.of(4, "a4", 0));
		rows.add(Row.of(5, "a5", 0));
		rows.add(Row.of(6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingNo1);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testOutstandingNo2() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "a1", 0));
		rows.add(Row.of(12, "a2", 0));
		rows.add(Row.of(3, "a3", 0));
		rows.add(Row.of(4, "a4", 0));
		rows.add(Row.of(5, "a5", 0));
		rows.add(Row.of(6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.AVG));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingTop2);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testAttribution() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1000, "a1", 0));
		rows.add(Row.of(1, "a2", 0));
		rows.add(Row.of(3, "a3", 0));
		rows.add(Row.of(4, "a4", 0));
		rows.add(Row.of(5, "a5", 0));
		rows.add(Row.of(6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			//.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Attribution);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testOutstandingNoLast() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(-10000, "a1", 0));
		rows.add(Row.of(-1, "a2", 0));
		rows.add(Row.of(-3, "a3", 0));
		rows.add(Row.of(-4, "a4", 0));
		rows.add(Row.of(-5, "a5", 0));
		rows.add(Row.of(-6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.OutstandingLast);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testEveness() {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of(10, "1", 0));
		rows.add(Row.of(11, "2", 0));
		rows.add(Row.of(9, "3", 0));
		rows.add(Row.of(10, "4", 0));
		rows.add(Row.of(9, "5", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Evenness);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testChangePoint() {
		List <Row> rows = new ArrayList <>();

		long step = 1;
		rows.add(Row.of(1, new Timestamp(1000 * step), 0));
		rows.add(Row.of(3, new Timestamp(2000 * step), 0));
		rows.add(Row.of(5, new Timestamp(3000 * step), 0));
		rows.add(Row.of(3, new Timestamp(4000 * step), 0));
		rows.add(Row.of(1, new Timestamp(5000 * step), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.ChangePoint);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testOutlier() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(-10000, new Timestamp(1000), 0));
		rows.add(Row.of(-1, new Timestamp(2000), 0));
		rows.add(Row.of(-3, new Timestamp(3000), 0));
		rows.add(Row.of(-4, new Timestamp(4000), 0));
		rows.add(Row.of(-5, new Timestamp(5000), 0));
		rows.add(Row.of(-6, new Timestamp(6000), 0));
		rows.add(Row.of(-1, new Timestamp(7000), 0));
		rows.add(Row.of(-3, new Timestamp(8000), 0));
		rows.add(Row.of(-4, new Timestamp(9000), 0));
		rows.add(Row.of(-5, new Timestamp(10000), 0));
		rows.add(Row.of(-6, new Timestamp(11000), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Subject subject = new Subject()
			//.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Outlier);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testSeasonality() {
		List <Row> rows = new ArrayList <>();

		//long step = 86400;
		long step = 60 * 60 * 24;
		rows.add(Row.of(1, new Timestamp(1000 * step), 0));
		rows.add(Row.of(2, new Timestamp(2000 * step), 0));
		rows.add(Row.of(3, new Timestamp(3000 * step), 0));
		rows.add(Row.of(4, new Timestamp(4000 * step), 0));
		rows.add(Row.of(1, new Timestamp(5000 * step), 0));
		rows.add(Row.of(2, new Timestamp(6000 * step), 0));
		rows.add(Row.of(3, new Timestamp(7000 * step), 0));
		rows.add(Row.of(4, new Timestamp(8000 * step), 0));
		rows.add(Row.of(1, new Timestamp(9000 * step), 0));
		rows.add(Row.of(2, new Timestamp(10000 * step), 0));
		rows.add(Row.of(3, new Timestamp(11000 * step), 0));
		rows.add(Row.of(4, new Timestamp(12000 * step), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Seasonality);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testOTrend() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(1, new Timestamp(1000), 0));
		rows.add(Row.of(2, new Timestamp(2000), 0));
		rows.add(Row.of(3, new Timestamp(3000), 0));
		rows.add(Row.of(4, new Timestamp(4000), 0));
		rows.add(Row.of(5, new Timestamp(5000), 0));
		rows.add(Row.of(6, new Timestamp(6000), 0));
		rows.add(Row.of(1, new Timestamp(7000), 0));
		rows.add(Row.of(7, new Timestamp(8000), 0));
		rows.add(Row.of(8, new Timestamp(9000), 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");
		source.print();

		Subject subject = new Subject()
			.addSubspace(new Subspace("label", 0))
			.setBreakdown(new Breakdown("col1"))
			.addMeasure(new Measure("col0", MeasureAggr.SUM));

		Insight insight = Mining.calcInsight(source, subject, InsightType.Trend);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testBasicStat() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "a1", 0));
		rows.add(Row.of(1, "a2", 0));
		rows.add(Row.of(3, "a3", 0));
		rows.add(Row.of(4, "a4", 0));
		rows.add(Row.of(5, "a5", 0));
		rows.add(Row.of(6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Insight insight = StatInsight.basicStat(source, "label");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}

	@Test
	public void testDistribution() {
		List <Row> rows = new ArrayList <>();

		rows.add(Row.of(15, "a1", 0));
		rows.add(Row.of(1, "a2", 0));
		rows.add(Row.of(3, "a3", 0));
		rows.add(Row.of(4, "a4", 0));
		rows.add(Row.of(5, "a5", 0));
		rows.add(Row.of(6, "a6", 0));

		LocalOperator <?> source = new MemSourceLocalOp(rows, "col0 int, col1 string, label int");

		Insight insight = StatInsight.distribution(source, new Breakdown("col1"), "col0");
		System.out.println(insight);
		System.out.println(JsonConverter.toJson(DvInsightDescription.of(insight)));
	}


	@Test
	public void testCrossMeasureCorrelation() {
		LocalOperator <?> source = new MemSourceLocalOp(MiningInsightTest.EMISSION, MiningInsightTest.EMISSION_SCHEMA);

		Subject subject = new Subject()
			.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.MAX))
			.addMeasure(new Measure("so", MeasureAggr.MAX));

		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.CrossMeasureCorrelation;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true).setNeedFilter(true);
		miningInsight.processData(source);

		System.out.println(miningInsight.insight);

		System.out.println(JsonConverter.toJson(DvInsightDescription.of(miningInsight.insight)));
	}

}