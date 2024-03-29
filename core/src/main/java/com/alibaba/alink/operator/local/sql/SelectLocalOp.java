package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.sql.SelectUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a batch operator.
 */
@NameCn("SQL操作：Select")
public final class SelectLocalOp extends BaseSqlApiLocalOp <SelectLocalOp>
	implements SelectParams <SelectLocalOp> {

	private static final long serialVersionUID = -1867376056670775636L;

	public SelectLocalOp() {
		this(new Params());
	}

	public SelectLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public SelectLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] colNames = in.getColNames();

		String clause = getClause();
		String newClause = SelectUtils.convertRegexClause2ColNames(colNames, clause);

		if (SelectUtils.isSimpleSelect(newClause, colNames)) {
			Tuple2 <String[], String[]> sTuple = SelectUtils.splitAndTrim(clause, colNames);

			int[] colIndexes = TableUtil.findColIndicesWithAssertAndHint(in.getSchema(), sTuple.f0);
			MTable mt = MTableUtil.selectAs(in.getOutputTable(), colIndexes, sTuple.f1);

			this.setOutputTable(mt);
		} else {
			this.setOutputTable(
				LocalMLEnvironment.getInstance().getSqlExecutor().select(in, newClause).getOutputTable());
		}
	}
}
