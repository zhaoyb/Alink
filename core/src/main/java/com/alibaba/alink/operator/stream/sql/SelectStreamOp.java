package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.sql.SelectMapper;
import com.alibaba.alink.operator.common.sql.SelectUtils;
import com.alibaba.alink.operator.common.sql.SimpleSelectMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a stream operator.
 */
@NameCn("SQL操作：Select")
@NameEn("SQL：Select")
public final class SelectStreamOp extends MapStreamOp <SelectStreamOp>
	implements SelectParams <SelectStreamOp> {

	private static final long serialVersionUID = 7401063240614374140L;

	public SelectStreamOp() {
		this(new Params());
	}

	public SelectStreamOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public SelectStreamOp(Params params) {
		super(SelectMapper::new, params);
	}
	
}

