package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Union with another <code>BatchOperator</code>. The duplicated records are kept.
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, isRepeated = true))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@NameCn("SQL操作：UnionAll")
public final class UnionAllLocalOp extends LocalOperator <UnionAllLocalOp> {

	private static final long serialVersionUID = 2468662188701775196L;

	public UnionAllLocalOp() {
		this(new Params());
	}

	public UnionAllLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkMinOpSize(1, inputs);
		List <Row> result = new ArrayList <>();
		for (LocalOperator <?> input : inputs) {
			result.addAll(input.getOutputTable().getRows());
		}
		setOutputTable(new MTable(result, inputs[0].getOutputTable().getSchemaStr()));
	}
}
