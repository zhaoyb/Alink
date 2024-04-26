package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class RiskAlikeBuildGraphBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Row[] edges = new Row[] {
			Row.of(1L, 2L, 1D),
			Row.of(1L, 3L, 1D),
			Row.of(1L, 4L, 1D),
			Row.of(2L, 3L, 1D),
			Row.of(2L, 4L, 1D),
			Row.of(3L, 4L, 1D),
			Row.of(4L, 5L, 1D),
			Row.of(5L, 6L, 1D),
			Row.of(5L, 7L, 1D),
			Row.of(5L, 8L, 1D),
			Row.of(6L, 7L, 1D),
			Row.of(6L, 8L, 1D),
			Row.of(7L, 8L, 1D)
		};

		BatchOperator edgeData = new MemSourceBatchOp(edges, new String[] {"source", "target", "weight"});

		Row[] vertex = new Row[]{Row.of(1L)};
		BatchOperator vertexData = new MemSourceBatchOp(vertex, new String[] {"vertex"});

		RiskAlikeBuildGraphBatchOp louvainBatchOp = new RiskAlikeBuildGraphBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setVertexCol("vertex")
			.setExpandDegree(1)
			.linkFrom(vertexData, edgeData);
		louvainBatchOp.lazyPrint();
		louvainBatchOp.getSideOutput(0).print();
	}
}
