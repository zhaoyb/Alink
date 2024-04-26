package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SampleWithSizeBatchOpTest extends AlinkTestBase {
	public static Table getBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(4.0, 2.0),
				Row.of(null, null),
				Row.of(1.0, 2.0),
				Row.of(-1.0, -3.0),
				Row.of(4.0, 2.0),
				Row.of(null, null),
				Row.of(1.0, 2.0),
				Row.of(-1.0, -3.0),
				Row.of(4.0, 2.0),
				Row.of(null, null)
			};
		String[] colNames = new String[] {"f0", "f1"};
		return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
	}

	@Test
	public void test() throws Exception {
		//AlinkGlobalConfiguration.setPrintProcessInfo(true);
		TableSourceBatchOp tableSourceBatchOp = new TableSourceBatchOp(getBatchTable());
		long cnt = tableSourceBatchOp.link(new SampleWithSizeBatchOp(5, true)).count();
		Assert.assertEquals(5, cnt);
	}

	public void test2() throws Exception {
		//AlinkGlobalConfiguration.setPrintProcessInfo(true);
		TableSourceBatchOp tableSourceBatchOp = new TableSourceBatchOp(getBatchTable());
		long cnt = tableSourceBatchOp.link(new SampleWithSizeBatchOp(20, true)).count();
		Assert.assertEquals(20, cnt);
	}

	@Test
	public void testWithoutReplace() throws Exception {
		TableSourceBatchOp tableSourceBatchOp = new TableSourceBatchOp(getBatchTable());
		long cnt = tableSourceBatchOp.link(new SampleWithSizeBatchOp(5, false)).count();
		Assert.assertEquals(5, cnt);
	}

	@Test
	public void testWithoutReplace2() throws Exception {
		TableSourceBatchOp tableSourceBatchOp = new TableSourceBatchOp(getBatchTable());
		long cnt = tableSourceBatchOp.link(new SampleWithSizeBatchOp(20, false)).count();
		Assert.assertEquals(10, cnt);
	}

	@Test
	public void testSampleCount() {
		long totalCount = 100;
		//long[] countsByTask = new long[] {33, 67};
		long[] countsByTask = new long[] {4, 96};
		long sampleCount = 13;

		int[] counts = SampleWithSizeBatchOp.getSampleCountByTask(totalCount, countsByTask, sampleCount);
		System.out.println(JsonConverter.toJson(counts));
	}

}