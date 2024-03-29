package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.graph.LouvainParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES)
})
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("Louvain社区发现")
@NameEn("Louvain Community Detect")
public class LouvainBatchOp extends BatchOperator <LouvainBatchOp> implements LouvainParams <LouvainBatchOp> {

	private static final long serialVersionUID = -3990487225537358585L;

	public LouvainBatchOp(Params params) {
		super(params);
	}

	public LouvainBatchOp() {
		super(new Params());
	}

	@Override
	public LouvainBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(1, inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String edgeWeightCol = getEdgeWeightCol();
		Integer maxIter = getMaxIter();
		Integer minChange = getChangedNodeNumThreshold();
		//Double threshold = getModularityThreshold();
		String[] outputCols = new String[] {"vertex", "label"};
		Boolean hasEdgeWeight = true;
		if (null == edgeWeightCol) {
			hasEdgeWeight = false;
		}
		String[] inputCols = new String[] {sourceCol, targetCol, edgeWeightCol};
		if (!hasEdgeWeight) {
			inputCols = new String[] {sourceCol, targetCol};
		}
		DataSet<Row> inputRows = inputs[0].select(inputCols).getDataSet();
		TypeInformation[] types = TableUtil.findColTypes(inputs[0].getSchema(), inputCols);
		if (types.length == 3 && types[2] != Types.DOUBLE) {
			throw new RuntimeException(String.format("Edge input data, weightCol should be double"));
		}
		// edges with source, target, weight, community1, community2, community2 tot initial with 0
		Boolean finalHasEdgeWeight = hasEdgeWeight;
		DataSet <Tuple6 <Long, Long, Double, Long, Long, Double>> workSet = inputRows.flatMap(
			new FlatMapFunction <Row, Tuple6 <Long, Long, Double, Long, Long, Double>>() {
				@Override
				public void flatMap(Row value, Collector <Tuple6 <Long, Long, Double, Long, Long, Double>> out)
					throws Exception {
					Long leftNodeId = Long.valueOf(String.valueOf(value.getField(0)));
					Long rightNodeId = Long.valueOf(String.valueOf(value.getField(1)));
					double weight = 1.0;
					if (finalHasEdgeWeight) {
						weight = (Double) value.getField(2);
					}
					out.collect(Tuple6.of(leftNodeId, rightNodeId, weight, leftNodeId, rightNodeId, 0.0D));
					out.collect(Tuple6.of(rightNodeId, leftNodeId, weight, rightNodeId, leftNodeId, 0.0D));
				}
			}).name("flat_edges");

		// node and community distinct set
		DataSet<Tuple2<Long, Long>> solutionSet = workSet.project(0, 3);
		solutionSet = solutionSet.distinct();

		DeltaIteration <Tuple2<Long, Long>, Tuple6 <Long, Long, Double, Long, Long, Double>> deltaIteration = solutionSet.iterateDelta(workSet, maxIter, 0);

		DataSet<Tuple2<Long, Double>> communityTot = deltaIteration.getWorkset().flatMap(
			new FlatMapFunction <Tuple6 <Long, Long, Double, Long, Long, Double>, Tuple2 <Long, Double>>() {
				@Override
				public void flatMap(Tuple6 <Long, Long, Double, Long, Long, Double> value,
									Collector <Tuple2 <Long, Double>> out) throws Exception {
					if (value.f3 != value.f4) {
						out.collect(Tuple2.of(value.f3, value.f2));
					}
				}
			}
		).groupBy(0).aggregate(Aggregations.SUM, 1).name("compute_community_tot");

		DataSet<Tuple6 <Long, Long, Double, Long, Long, Double>> vertexWithCommunityTot = deltaIteration.getWorkset().join(communityTot, JoinHint.REPARTITION_HASH_SECOND)
			.where(4).equalTo(0)
			.projectFirst(0,1,2,3,4)
			.projectSecond(1);

		DataSet <Tuple1 <Double>> m = deltaIteration.getWorkset().aggregate(Aggregations.SUM, 2).project(2);
		DataSet<Tuple3<Long, Long, Boolean>> newCommunityWithTag = vertexWithCommunityTot.groupBy(0)
			.reduceGroup(
				new RichGroupReduceFunction <Tuple6 <Long, Long, Double, Long, Long, Double>, Tuple3<Long, Long, Boolean>>() {
					private List <Tuple1<Double>> m = null;
					private double mValue = 0;
					private double mSquare = 0;

					@Override
					public void open(Configuration parameters) throws Exception {
						this.m = getRuntimeContext().getBroadcastVariable("m");
						this.mValue = m.get(0).f0 * 0.5;
						this.mSquare = m.get(0).f0 * m.get(0).f0 * 0.5;
					}
					@Override
					public void reduce(Iterable <Tuple6 <Long, Long, Double, Long, Long, Double>> values,
									   Collector <Tuple3<Long, Long, Boolean>> out) throws Exception {
						List<Tuple6 <Long, Long, Double, Long, Long, Double>> list = new ArrayList <>();
						double ki = 0;
						long currentNode = 0;
						HashMap<Long, Double> communityWeight = new HashMap <>();
						for (Tuple6 <Long, Long, Double, Long, Long, Double> v : values) {
							ki += v.f2;
							if (v.f0 == v.f4 && v.f1 == v.f3 || v.f3 == v.f4) {
								return;
							}
							list.add(v);
							currentNode = v.f0;
							communityWeight.put(v.f4, communityWeight.getOrDefault(v.f4, 0D) + v.f2);
						}
						double maxDeltaQ = 0;
						long maxC = 0;
						for (int i = 0; i < list.size(); i++) {
							Tuple6 <Long, Long, Double, Long, Long, Double> t = list.get(i);
							long currentC = t.f4;
							double kiin = communityWeight.get(currentC);
							double deltaQ = kiin / mValue - t.f5 * ki / mSquare;
							if (deltaQ > maxDeltaQ) {
								maxDeltaQ = deltaQ;
								maxC = t.f4;
							}
						}
						if (maxDeltaQ > 0) {
							out.collect(Tuple3.of(currentNode, maxC, true));
							//System.out.println(String.format("deltaQ loop, node %s community %s, new", currentNode, maxC));
						}
					}
				}).withBroadcastSet(m, "m").name("group_compute_new_community");

		DataSet<Tuple2<Long, Long>> newCommunity = newCommunityWithTag.project(0, 1);
		DataSet <Integer> updateCount = newCommunityWithTag.reduceGroup(
			new GroupReduceFunction <Tuple3<Long, Long, Boolean>, Integer>() {
				@Override
				public void reduce(Iterable <Tuple3<Long, Long, Boolean>> values, Collector <Integer> out) throws Exception {
					int count = 0;
					for (Tuple3<Long, Long, Boolean> t : values) {
						if (t.f2) {
							count += 1;
						}
					}
					out.collect(Integer.valueOf(count));
				}
			}
		);

		DataSet<Tuple2<Long, Long>> deltaSolutionSet = newCommunity.join(deltaIteration.getSolutionSet())
			.where(0).equalTo(0)
			.projectSecond(0).projectFirst(1);

		DataSet<Tuple6 <Long, Long, Double, Long, Long, Double>> updateCommunityEdges = deltaIteration.getWorkset()
			.join(newCommunity, JoinHint.REPARTITION_HASH_SECOND).where(0).equalTo(0).with(
				new JoinFunction <Tuple6 <Long, Long, Double, Long, Long, Double>, Tuple2 <Long, Long>, Tuple6 <Long,
					Long, Double, Long, Long, Double>>() {
					@Override
					public Tuple6 <Long, Long, Double, Long, Long, Double> join(
						Tuple6 <Long, Long, Double, Long, Long, Double> first, Tuple2 <Long, Long> second)
						throws Exception {
						return Tuple6.of(first.f0, first.f1, first.f2, second.f1, first.f4, first.f5);
					}
				}
			).name("join_left_node_community")
			.join(newCommunity, JoinHint.REPARTITION_HASH_SECOND).where(1).equalTo(0).with(
				new JoinFunction <Tuple6 <Long, Long, Double, Long, Long, Double>, Tuple2 <Long, Long>, Tuple6 <Long,
					Long, Double, Long, Long, Double>>() {
					@Override
					public Tuple6 <Long, Long, Double, Long, Long, Double> join(
						Tuple6 <Long, Long, Double, Long, Long, Double> first, Tuple2 <Long, Long> second)
						throws Exception {
						//System.out.println(String.format("edge with new community, node %s, community %s, node %s, community %s",
						//	first.f0, first.f3, first.f1, second.f1));
						return Tuple6.of(first.f0, first.f1, first.f2, first.f3, second.f1, first.f5);
					}
				}
			).name("join_right_node_community");

		DataSet<Tuple6 <Long, Long, Double, Long, Long, Double>> newWorkSet = updateCommunityEdges.groupBy(3)
			.reduceGroup(
				new RichGroupReduceFunction <Tuple6 <Long, Long, Double, Long, Long, Double>, Tuple6 <Long, Long,
					Double, Long, Long, Double>>() {
					private int count = 0;

					@Override
					public void open(Configuration parameters) throws Exception {
						List<Integer> count = getRuntimeContext().getBroadcastVariable("count");
						this.count = count.get(0);
					}
					@Override
					public void reduce(Iterable <Tuple6 <Long, Long, Double, Long, Long, Double>> values,
									   Collector <Tuple6 <Long, Long, Double, Long, Long, Double>> out)
						throws Exception {
						if (count >= minChange) {
							for (Tuple6 <Long, Long, Double, Long, Long, Double> t : values) {
								out.collect(t);
							}
						} else {
							Long sourceNodeId = 0L;
							HashMap<Long, Double> weightsMap = new HashMap<>();
							for (Tuple6 <Long, Long, Double, Long, Long, Double> t : values) {
								sourceNodeId = t.f3;
								weightsMap.put(t.f4, weightsMap.getOrDefault(t.f4, 0D) + t.f2);
							}
							for (Entry <Long, Double> entry : weightsMap.entrySet()) {
								// 相同社区的边不输出
								if (sourceNodeId == entry.getKey()) {
									continue;
								}
								out.collect(Tuple6.of(sourceNodeId, entry.getKey(), entry.getValue(), sourceNodeId, entry.getKey(), 0D));
								//System.out.println(String.format("community collapse, node %s, node %s, weight %s",
								//	sourceNodeId, entry.getKey(), entry.getValue()));
							}
						}
					}
				}).withBroadcastSet(updateCount, "count");

		DataSet<Tuple2<Long, Long>> firstStageCommunity = deltaIteration.closeWith(deltaSolutionSet, newWorkSet);

		// bulk iteration, update final community
		IterativeDataSet<Tuple2<Long, Long>> bulkIteration = firstStageCommunity.iterate(maxIter);
		DataSet<Tuple3<Long, Long, Boolean>> updateLabelWithTag = bulkIteration.join(bulkIteration)
			.where(1).equalTo(0).with(
				new JoinFunction <Tuple2 <Long, Long>, Tuple2 <Long, Long>, Tuple3 <Long, Long, Boolean>>() {
					@Override
					public Tuple3 <Long, Long, Boolean> join(Tuple2 <Long, Long> first, Tuple2 <Long, Long> second)
						throws Exception {
						if (first.f0 == first.f1) {
							return Tuple3.of(first.f0, first.f1, false);
						} else if (first.f0 == second.f1 && first.f1 == second.f0) {
							return Tuple3.of(first.f0, Math.max(first.f0, first.f1), true);
						} else {
							return Tuple3.of(first.f0, second.f1, true);
						}
					}
				});
		DataSet<Tuple2<Long, Long>> updateLabel = updateLabelWithTag.project(0, 1);
		DataSet<Tuple1<Long>> changeNode = updateLabelWithTag.filter(
			new FilterFunction <Tuple3 <Long, Long, Boolean>>() {
				@Override
				public boolean filter(Tuple3 <Long, Long, Boolean> value) throws Exception {
					return value.f2;
				}
			}).project(0);
		DataSet<Tuple2<Long, Long>> finalCommunity = bulkIteration.closeWith(updateLabel, changeNode);

		DataSet <Row> res = finalCommunity.map(new MapFunction <Tuple2 <Long, Long>, Row>() {
			@Override
			public Row map(Tuple2 <Long, Long> value) throws Exception {
				return Row.of(value.f0, value.f1);
			}
		});
		this.setOutput(res, outputCols, new TypeInformation <?>[] {Types.LONG, Types.LONG});
		return this;
	}
}
