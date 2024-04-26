package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.sql.SelectMapper;
import com.alibaba.alink.operator.common.sql.SelectUtils;
import com.alibaba.alink.operator.common.sql.SimpleSelectMapper;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a batch operator.
 */
@NameCn("SQL操作：Select")
@NameEn("SQL Select Operation")
public final class SelectBatchOp extends MapBatchOp <SelectBatchOp>
	implements SelectParams <SelectBatchOp> {

	private static final long serialVersionUID = -1867376056670775636L;

	public SelectBatchOp() {
		this(new Params());
	}

	public SelectBatchOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public SelectBatchOp(Params params) {
		super(SelectMapper::new, params);
	}

}