package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface RiskAlikeBuildGraphParams<T> extends
	HasVertexCol<T>,
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

	@NameCn("扩散度数")
	@DescCn("从黑种子节点出发，在输入的边表上进行扩散的度数")
	ParamInfo <Integer> EXPAND_DEGREE = ParamInfoFactory
		.createParamInfo("expandDegree", Integer.class)
		.setDescription("expand degree on edges input")
		.setHasDefaultValue(2)
		.build();

	default Integer getExpandDegree() {return get(EXPAND_DEGREE);}

	default T setExpandDegree(Integer value) {return set(EXPAND_DEGREE, value);}

}
