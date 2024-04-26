package com.alibaba.alink.operator.local.sql;

import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Test;

public class OrderByLocalOpTest {
	@Test
	public void testOrderByLocalOp() {
		LocalOperator <?> data = IrisData.getLocalSourceOp();
		data
			.link(
				new OrderByLocalOp()
					.setLimit(10)
					.setClause("sepal_length")
			)
			.print();
	}
}