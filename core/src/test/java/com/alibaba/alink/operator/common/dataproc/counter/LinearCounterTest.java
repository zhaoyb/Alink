package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * UT for LinearCounter
 */
public class LinearCounterTest extends AlinkTestBase {
	@Test
	public void test() {
		LinearCounter counter1 = new LinearCounter(10);
		LinearCounter counter2 = new LinearCounter(counter1);

		for (int i = 0; i < 1000; i++) {
			counter1.visit(i);
			counter2.visit(i * 2);
		}

		AbstractCounter counter = counter1.merge(counter2);
		Assert.assertTrue(counter.count() > 1000 && counter.count() < 2000);
	}
}