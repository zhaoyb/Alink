package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs10;

public interface LouvainParams<T> extends
	HasMaxIterDefaultAs10 <T>,
	GraphVertexCols<T> {

	@NameCn("边权重列")
	@DescCn("表示边权重的列")
	ParamInfo <String> EDGE_WEIGHT_COL = ParamInfoFactory
		.createParamInfo("edgeWeightCol", String.class)
		.setDescription("edge weight column")
		.setHasDefaultValue(null)
		.build();

	default String getEdgeWeightCol() {return get(EDGE_WEIGHT_COL);}

	default T setEdgeWeightCol(String value) {return set(EDGE_WEIGHT_COL, value);}

	@NameCn("改变社区节点个数阈值")
	@DescCn("改变社区节点个数的阈值，低于阈值则进行图压缩")
	ParamInfo <Integer> CHANGED_NODE_NUM_THRESHOLD = ParamInfoFactory
		.createParamInfo("changedNodeNumThreshold", Integer.class)
		.setDescription("community changed node num threshold")
		.setHasDefaultValue(10000)
		.build();

	default Integer getChangedNodeNumThreshold() {return get(CHANGED_NODE_NUM_THRESHOLD);}

	default T setChangedNodeNumThreshold(Integer value) {return set(CHANGED_NODE_NUM_THRESHOLD, value);}

	//@NameCn("模块度收敛阈值")
	//@DescCn("模块度收敛阈值，低于阈值退出迭代")
	//ParamInfo <Double> MODULARITY_THRESHOLD = ParamInfoFactory
	//	.createParamInfo("modularityThreshold", Double.class)
	//	.setDescription("Modularity threshold")
	//	.setHasDefaultValue(0.01)
	//	.build();
	//
	//default Double getModularityThreshold() {return get(MODULARITY_THRESHOLD);}
	//
	//default T setModularityThreshold(Double value) {return set(MODULARITY_THRESHOLD, value);}
}
