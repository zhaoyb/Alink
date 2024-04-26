package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple3;

import breeze.stats.distributions.LogNormal;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.LogisticDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;

import java.util.Arrays;
import java.util.List;

public class DistributionUtil {

	public static final KolmogorovSmirnovTest KS_TEST = new KolmogorovSmirnovTest();

	public static double[] loadDataToVec(List <Number> dataList) {
		double[] datas = new double[dataList.size()];
		for (int i = 0; i < dataList.size(); i++) {
			datas[i] = Double.valueOf(String.valueOf(dataList.get(i)));
		}
		return datas;
	}

	public static Tuple3 <Double, Double, double[]> getMeanSd(List <Number> dataList) {
		if (dataList.size() == 0) {
			return Tuple3.of(0D, 0D, null);
		}
		double[] datas = loadDataToVec(dataList);
		double avg = Arrays.stream(datas).average().getAsDouble();
		double variance = 0;
		for (int i = 0; i < datas.length; i++) {
			variance += Math.pow(datas[i] - avg, 2);
		}
		variance = variance / datas.length;
		double sd = Math.sqrt(variance);
		return Tuple3.of(avg, sd, datas);
	}

	public static double testNormalDistribution(List <Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		Tuple3 <Double, Double, double[]> avgTuple = getMeanSd(dataList);
		NormalDistribution distribution = new NormalDistribution(avgTuple.f0, avgTuple.f1);
		return KS_TEST.kolmogorovSmirnovTest(distribution, avgTuple.f2);
	}

	public static double testUniformDistribution(List <Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		double[] datas = loadDataToVec(dataList);
		double lower = Arrays.stream(datas).min().getAsDouble();
		double upper = Arrays.stream(datas).max().getAsDouble();

		UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
		return KS_TEST.kolmogorovSmirnovTest(distribution, datas);
	}

	// if positive, return degreeOfFreedom
	public static double testChiSquaredDistribution(List <Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		double[] datas = loadDataToVec(dataList);
		for (int i = 1; i <= 20; i++) {
			ChiSquaredDistribution distribution = new ChiSquaredDistribution(i);
			double p = KS_TEST.kolmogorovSmirnovTest(distribution, datas);
			if (p > 0.05 ) {
				return i;
			}
		}
		return 0;
	}

	public static double testExpDistribution(List <Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		double[] datas = loadDataToVec(dataList);
		double avg = Arrays.stream(datas).average().getAsDouble();
		ExponentialDistribution distribution = new ExponentialDistribution(avg);
		return KS_TEST.kolmogorovSmirnovTest(distribution, datas);
	}

	// if positive, return degreeOfFreedom
	public static double testTDistribution(List <Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		double[] datas = loadDataToVec(dataList);
		for (int i = 1; i <= 20; i++) {
			TDistribution distribution = new TDistribution(i);
			double p = KS_TEST.kolmogorovSmirnovTest(distribution, datas);
			if (p > 0.05) {
				return i;
			}
		}
		return 0;
	}

	public static double testLogisticDistribution(List<Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		Tuple3 <Double, Double, double[]> tuple3 = getMeanSd(dataList);
		double avg = tuple3.f0;
		double sd = tuple3.f1;
		double[] datas = tuple3.f2;
		LogisticDistribution distribution = new LogisticDistribution(avg, sd);
		return KS_TEST.kolmogorovSmirnovTest(distribution, datas);
	}

	public static double testLogNormalDistribution(List<Number> dataList) {
		if (dataList.size() == 0) {
			return 0;
		}
		double[] datas = loadDataToVec(dataList);
		double min = Arrays.stream(datas).min().getAsDouble();
		if (min <= 0) {
			return 0;
		}
		double[] logValues = new double[datas.length];
		for (int i = 0; i < datas.length; i++) {
			logValues[i] = Math.log(datas[i]);
		}
		double avg = Arrays.stream(logValues).average().getAsDouble();
		double var = 0;
		for (int i = 0; i < logValues.length; i++) {
			var += Math.pow((logValues[i] - avg), 2);
		}
		var = Math.sqrt(var);
		LogNormalDistribution distribution = new LogNormalDistribution(avg, var);
		return KS_TEST.kolmogorovSmirnovTest(distribution, datas);
	}


}
