package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.sql.SelectMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a batch operator.
 */
@NameCn("SQL操作：Select")
public final class SelectLocalOp extends MapLocalOp <SelectLocalOp>
	implements SelectParams <SelectLocalOp> {

	private static final long serialVersionUID = -1867376056670775636L;

	public SelectLocalOp() {
		this(new Params());
	}

	public SelectLocalOp(Params params) {
		super(SelectMapper::new, params);
	}

}
