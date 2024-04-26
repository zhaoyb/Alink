package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.AsParams;
import org.apache.commons.lang.StringUtils;

/**
 * Rename the fields of a batch operator.
 */
@NameCn("SQL操作：As")
public final class AsLocalOp extends BaseSqlApiLocalOp <AsLocalOp>
	implements AsParams <AsLocalOp> {

	private static final long serialVersionUID = -6266483708473673388L;

	public AsLocalOp() {
		this(new Params());
	}

	public AsLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public AsLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		String[] splits = StringUtils.split(getClause(), ",");
		int[] colIndexes = new int[splits.length];
		for (int i = 0; i < splits.length; i++) {
			splits[i] = splits[i].trim().replace("`", "");
			colIndexes[i] = i;
		}

		MTable mt = MTableUtil.selectAs(in.getOutputTable(), colIndexes, splits);

		this.setOutputTable(mt);
	}
}
