package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.HaskernelType.KernelType;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;
import com.alibaba.alink.params.outlier.OutlierDetectorParams;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

public class OcsvmDetectorTest extends AlinkTestBase {

	private static final double EPS = 1e-12;

	@Test
	public void testDetectDefaultThreshold() throws Exception {
		Row[] data = new Row[] {
			Row.of("1.21, 1.30"),
			Row.of("1.22, 1.11"),
			Row.of("1.23, 1.27"),
			Row.of("1.14, 1.36"),
			Row.of("1.35, 1.21"),
			Row.of("1.23, 1.16"),
			Row.of("1.52, 1.23"),
			Row.of("1.41, 1.14"),
			Row.of("1.32, 1.13"),
			Row.of("1.23, 1.20"),
			Row.of("1.25, 1.35"),
			Row.of("1.16, 1.34"),
			Row.of("122.3, 144.3"),
			Row.of("123.2, 112.1"),
			Row.of("134.1, 134.3")

		};
		MTable mtable = new MTable(data, "vec vector");
		TableSchema schema = TableUtil.schemaStr2Schema("mtable string");

		Params params = new Params()
			.set(HasInputMTableCol.INPUT_MTABLE_COL, "mtable")
			.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "dummy")
			.set(OutlierDetectorParams.PREDICTION_DETAIL_COL, "detail")
			.set(HasVectorCol.VECTOR_COL, "vec")
			.set(OcsvmDetectorParams.KERNEL_TYPE, KernelType.RBF)
			.set(OcsvmDetectorParams.GAMMA, 0.1)
			.set(OcsvmDetectorParams.NU, 0.4);
		OcsvmDetector detector = new OcsvmDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		Tuple3 <Boolean, Double, Map <String, String>>[] expected = new Tuple3[] {
			Tuple3.of(false, -2.9943042603019343, null),
			Tuple3.of(false, -2.9876686889849307, null),
			Tuple3.of(false, -2.9963384851736583, null),
			Tuple3.of(false, -2.982890547267676, null),
			Tuple3.of(false, -2.9880841374659264, null),
			Tuple3.of(false, -2.993707255816288, null),
			Tuple3.of(false, -2.9469064418039776, null),
			Tuple3.of(false, -2.972330960082365, null),
			Tuple3.of(false, -2.985641230366144, null),
			Tuple3.of(false, -2.996339247195407, null),
			Tuple3.of(false, -2.988920681475392, null),
			Tuple3.of(false, -2.9875489813688842, null),
			Tuple3.of(true, 2.9963384851736583, null),
			Tuple3.of(true, 2.9963384851736583, null),
			Tuple3.of(true, 2.9963384851736583, null)
		};

		Assert.assertEquals(expected.length, results.length);
		for (int i = 0; i < results.length; ++i) {
			Assert.assertEquals(expected[i].f0, results[i].f0);
			Assert.assertEquals(expected[i].f1, results[i].f1, EPS);
			Assert.assertEquals(expected[i].f2, results[i].f2);
		}
	}

	@Test
	public void testDetectSparseVector() throws Exception {
		int n = 30;
		int len = 20;
		double sparsity = 0.1;
		Random random = new Random(0);
		Row[] data = new Row[n];
		for (int k = 0; k < n; k += 1) {
			SparseVector vector = new SparseVector(len);
			int lastIdx = 0;
			for (int i = 0; i < len * sparsity; i += 1) {
				int idx = lastIdx + random.nextInt(len - lastIdx);
				vector.set(idx, random.nextDouble());
				lastIdx = idx;
				if (lastIdx == len - 1) {
					break;
				}
			}
			data[k] = Row.of(vector);
		}

		MTable mtable = new MTable(data, "vec vector");
		TableSchema schema = TableUtil.schemaStr2Schema("mtable string");

		Params params = new Params()
			.set(HasInputMTableCol.INPUT_MTABLE_COL, "mtable")
			.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "dummy")
			.set(OutlierDetectorParams.PREDICTION_DETAIL_COL, "detail")
			.set(HasVectorCol.VECTOR_COL, "vec")
			.set(OcsvmDetectorParams.KERNEL_TYPE, KernelType.RBF)
			.set(OcsvmDetectorParams.GAMMA, 0.1)
			.set(OcsvmDetectorParams.NU, 0.4);
		OcsvmDetector detector = new OcsvmDetector(schema, params);
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detector.detect(mtable, false);

		for (Tuple3 <Boolean, Double, Map <String, String>> result : results) {
			System.out.println(result);
		}
		Assert.assertEquals(data.length, results.length);
	}
}
