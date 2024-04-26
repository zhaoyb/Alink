package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.params.graph.RiskAlikeBuildGraphParams;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.GRAPH_VERTICES),
	@PortSpec(value = PortType.DATA, desc = PortDesc.GRPAH_EDGES)
})
@ParamSelectColumnSpec(name = "vertexCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 1)

@NameCn("Risk Alike构图")
@NameEn("Risk Alike Build Graph")
public class RiskAlikeBuildGraphBatchOp extends BatchOperator <RiskAlikeBuildGraphBatchOp> implements
	RiskAlikeBuildGraphParams <RiskAlikeBuildGraphBatchOp> {

	private static final long serialVersionUID = 4374473007735445933L;

	public RiskAlikeBuildGraphBatchOp(Params params) {
		super(params);
	}

	public RiskAlikeBuildGraphBatchOp() {
		super(new Params());
	}

	@Override
	public RiskAlikeBuildGraphBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String edgeWeightCol = getEdgeWeightCol();
		String vertexCol = getVertexCol();
		Integer expandDegree = getExpandDegree();
		Boolean hasWeight = edgeWeightCol != null;
		String[] edgeCols = null;
		if (hasWeight) {
			edgeCols = new String[]{sourceCol, targetCol, edgeWeightCol};
		} else {
			edgeCols = new String[]{sourceCol, targetCol};
		}
		DataSet <Tuple5 <Long, Long, Double, Boolean, Boolean>> oneDirectEdges = inputs[1].select(edgeCols).getDataSet()
			.map(new MapFunction <Row, Tuple5 <Long, Long, Double, Boolean, Boolean>>() {

				@Override
				public Tuple5 <Long, Long, Double, Boolean, Boolean> map(Row row) throws Exception {
					Long sourceId = Long.valueOf(String.valueOf(row.getField(0)));
					Long targetId = Long.valueOf(String.valueOf(row.getField(1)));
					Double weight = 1.0D;
					if (hasWeight) {
						weight = Double.valueOf(String.valueOf(row.getField(2)));
					}
					return Tuple5.of(sourceId, targetId, weight, false, false);
				}
			});
		DataSet <Tuple5 <Long, Long, Double, Boolean, Boolean>> edges = oneDirectEdges.flatMap(
				new FlatMapFunction <Tuple5<Long, Long, Double, Boolean, Boolean>, Tuple5<Long, Long, Double, Boolean, Boolean>>() {

					@Override
					public void flatMap(
						Tuple5 <Long, Long, Double, Boolean, Boolean> t,
						Collector <Tuple5 <Long, Long, Double, Boolean, Boolean>> collector) throws Exception {
						collector.collect(t);
						collector.collect(Tuple5.of(t.f1, t.f0, t.f2, t.f4, t.f3));
					}
				});

		DataSet<Tuple2<Long, Boolean>> vertexInput = inputs[0].select(vertexCol).getDataSet()
			.map(new MapFunction <Row, Tuple2 <Long, Boolean>>() {
				@Override
				public Tuple2 <Long, Boolean> map(Row row) throws Exception {
					return Tuple2.of(Long.valueOf(String.valueOf(row.getField(0))), true);
				}
			});

		DataSet<Tuple2 <Long, Boolean>> vertexWithLabel = oneDirectEdges.flatMap(
			new FlatMapFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Tuple2 <Long, Boolean>>() {
				@Override
				public void flatMap(Tuple5 <Long, Long, Double, Boolean, Boolean> t,
									Collector <Tuple2 <Long, Boolean>> collector) throws Exception {
					collector.collect(Tuple2.of(t.f0, t.f3));
					collector.collect(Tuple2.of(t.f1, t.f4));
				}
			}).distinct()
			.leftOuterJoin(vertexInput).where(0).equalTo(0).with(
				new JoinFunction <Tuple2 <Long, Boolean>, Tuple2 <Long, Boolean>, Tuple2 <Long, Boolean>>() {
					@Override
					public Tuple2 <Long, Boolean> join(Tuple2 <Long, Boolean> left,
													   Tuple2 <Long, Boolean> right) throws Exception {
						if (right == null) {
							return left;
						}
						return Tuple2.of(left.f0, right.f1);
					}
				});

		IterativeDataSet <Tuple2<Long, Boolean>> iterationDataSet =  vertexWithLabel.iterate(expandDegree);

		DataSet <Tuple5 <Long, Long, Double, Boolean, Boolean>> edgesWithLabel = edges.leftOuterJoin(iterationDataSet)
			.where(0).equalTo(0).with(
				new JoinFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Tuple2 <Long, Boolean>, Tuple5 <Long,
					Long, Double, Boolean, Boolean>>() {
					@Override
					public Tuple5 <Long, Long, Double, Boolean, Boolean> join(
						Tuple5 <Long, Long, Double, Boolean, Boolean> left,
						Tuple2 <Long, Boolean> right) throws Exception {
						return Tuple5.of(left.f0, left.f1, left.f2, right.f1, right.f1);
					}
				});

		DataSet<Tuple2 <Long, Boolean>> changeVertexLabel = edgesWithLabel.flatMap(
			new FlatMapFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Tuple2 <Long, Boolean>>() {
				@Override
				public void flatMap(Tuple5 <Long, Long, Double, Boolean, Boolean> t,
									Collector <Tuple2 <Long, Boolean>> collector) throws Exception {
					if (t.f3) {
						collector.collect(Tuple2.of(t.f1, t.f3));
					}
				}
			}).distinct();
		DataSet<Tuple2 <Long, Boolean>> udpateVertexLabel = iterationDataSet.leftOuterJoin(changeVertexLabel).where(0).equalTo(0)
			.with(new JoinFunction <Tuple2 <Long, Boolean>, Tuple2 <Long, Boolean>, Tuple2 <Long, Boolean>>() {
				@Override
				public Tuple2 <Long, Boolean> join(Tuple2 <Long, Boolean> left,
												   Tuple2 <Long, Boolean> right) throws Exception {
					if (right == null) {
						return left;
					}
					return Tuple2.of(left.f0, right.f1);
				}
			});
		DataSet<Tuple2 <Long, Boolean>> finalVertexLabel = iterationDataSet.closeWith(udpateVertexLabel);

		DataSet <Row> vertex = finalVertexLabel.map(new MapFunction <Tuple2 <Long, Boolean>, Row>() {
			@Override
			public Row map(Tuple2 <Long, Boolean> t) throws Exception {
				Row row = new Row(2);
				row.setField(0, t.f0);
				row.setField(1, t.f1);
				return row;
			}
		});
		this.setOutput(vertex, new String[]{"user_id", "is_black"}, new TypeInformation[]{Types.LONG, Types.BOOLEAN});

		DataSet <Tuple5 <Long, Long, Double, Boolean, Boolean>> edgesWithFinalLabel = oneDirectEdges.leftOuterJoin(finalVertexLabel)
			.where(0).equalTo(0).with(
				new JoinFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Tuple2 <Long, Boolean>, Tuple5 <Long,
					Long, Double, Boolean, Boolean>>() {
					@Override
					public Tuple5 <Long, Long, Double, Boolean, Boolean> join(
						Tuple5 <Long, Long, Double, Boolean, Boolean> left,
						Tuple2 <Long, Boolean> right) throws Exception {
						return Tuple5.of(left.f0, left.f1, left.f2, right.f1, left.f4);
					}
				}).leftOuterJoin(finalVertexLabel)
			.where(1).equalTo(0).with(
				new JoinFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Tuple2 <Long, Boolean>, Tuple5 <Long,
					Long, Double, Boolean, Boolean>>() {
					@Override
					public Tuple5 <Long, Long, Double, Boolean, Boolean> join(
						Tuple5 <Long, Long, Double, Boolean, Boolean> left,
						Tuple2 <Long, Boolean> right) throws Exception {
						return Tuple5.of(left.f0, left.f1, left.f2, left.f3, right.f1);
					}
				});
		DataSet<Row> edgesOutput = edgesWithFinalLabel.map(
			new MapFunction <Tuple5 <Long, Long, Double, Boolean, Boolean>, Row>() {
				@Override
				public Row map(Tuple5 <Long, Long, Double, Boolean, Boolean> t)
					throws Exception {
					Row row = new Row(5);
					row.setField(0, t.f0);
					row.setField(1, t.f1);
					row.setField(2, t.f2);
					row.setField(3, t.f3);
					row.setField(4, t.f4);
					return row;
				}
			});

		String[] edgeOutputCols = new String[]{"user_id", "user_id_b", "prediction_score", "left_tag", "right_tag"};
		this.setSideOutputTables(new Table[]{
			DataSetConversionUtil.toTable(getMLEnvironmentId(), edgesOutput, edgeOutputCols,
			new TypeInformation[]{Types.LONG, Types.LONG, Types.DOUBLE, Types.BOOLEAN, Types.BOOLEAN})
		});
		return this;
	}
}
