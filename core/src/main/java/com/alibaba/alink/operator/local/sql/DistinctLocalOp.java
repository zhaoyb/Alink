package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.DistinctParams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remove duplicated records.
 */
@NameCn("SQL操作：Distinct")
public final class DistinctLocalOp extends BaseSqlApiLocalOp <DistinctLocalOp>
	implements DistinctParams <DistinctLocalOp> {

	private static final long serialVersionUID = 2774293287356122519L;

	public DistinctLocalOp() {
		this(new Params());
	}

	public DistinctLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		//this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor().distinct(inputs[0]).getOutputTable());
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedCols = getSelectedCols();
		final int[] selectedIndexes =
			(null == selectedCols) ? null : TableUtil.findColIndices(in.getColNames(), selectedCols);

		final int numThreads = LocalOperator.getParallelism();
		final TaskRunner taskRunner = new TaskRunner();
		List <Row> rows = in.getOutputTable().getRows();
		final int numRows = rows.size();

		ConcurrentHashMap <Row, Integer> map = new ConcurrentHashMap <>();
		{
			for (int k = 0; k < numThreads; ++k) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(k, numThreads, numRows);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(k, numThreads, numRows);
				taskRunner.submit(() -> {
					for (int index = start; index < start + cnt; index += 1) {
						Row row = rows.get(index);
						if (null == selectedIndexes) {
							map.putIfAbsent(row, 1);
						} else {
							map.putIfAbsent(Row.project(row, selectedIndexes), 1);
						}
					}
				});
			}
			taskRunner.join();
		}

		List <Row> result = new ArrayList <>();
		for (Row row : map.keySet()) {
			result.add(row);
		}

		if (null == selectedCols) {
			this.setOutputTable(new MTable(result, in.getOutputTable().getSchemaStr()));
		} else {
			TableSchema schema = new TableSchema(selectedCols, TableUtil.findColTypes(in.getSchema(), selectedCols));
			this.setOutputTable(new MTable(result, schema));
		}

	}

}
