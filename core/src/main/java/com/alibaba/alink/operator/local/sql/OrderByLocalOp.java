package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.sql.OrderUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.OrderByParams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Order the batch operator.
 */
@NameCn("SQL操作：OrderBy")
public final class OrderByLocalOp extends BaseSqlApiLocalOp <OrderByLocalOp>
	implements OrderByParams <OrderByLocalOp> {

	private static final long serialVersionUID = -8600279903752321912L;

	public OrderByLocalOp() {
		this(new Params());
	}

	public OrderByLocalOp(Params params) {
		super(params);
	}

	private int getOrderByParamWithDefault(ParamInfo <Integer> paramInfo) {
		int value = -1;
		if (this.getParams().contains(paramInfo)) {
			Integer v = this.getParams().get(paramInfo);
			if (v != null) {
				value = v;
			}
		}
		return value;
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		MTable mt = inputs[0].getOutputTable();

		int limit = getOrderByParamWithDefault(OrderByParams.LIMIT);
		int offset = getOrderByParamWithDefault(OrderByParams.OFFSET);
		int fetch = getOrderByParamWithDefault(OrderByParams.FETCH);

		if (0 == limit
			|| (limit < 0 && fetch == 0)
			|| (limit < 0 && offset >= mt.getNumRow())
		) {
			this.setOutputTable(new MTable(new ArrayList <>(), mt.getSchemaStr()));
			return;
		}

		String defaultOrder = getOrder();

		if (!defaultOrder.trim().equalsIgnoreCase("asc") &&
			!defaultOrder.trim().equalsIgnoreCase("desc")
		) {
			throw new AkIllegalArgumentException("Default order must be asc or desc.");
		}

		boolean isAscending = defaultOrder.trim().equalsIgnoreCase("asc");
		String orderClause = getClause();

		TableSchema schema = inputs[0].getSchema();

		// check cols.
		Tuple2 <String[], boolean[]> t2 = OrderUtils.getColsAndOrders(orderClause, schema, isAscending);
		String[] selectCols = t2.f0;
		boolean[] orders = t2.f1;

		final TypeComparator <Row> comparator = new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames())
			.createComparator(TableUtil.findColIndices(schema, selectCols), orders, 0, new ExecutionConfig());

		List <Row> rows;
		if (mt.getNumRow() >= 20 * 10000) {
			rows = mt.getRows()
				.parallelStream()
				.sorted(comparator::compare)
				.collect(Collectors.toList());
		} else {
			rows = new ArrayList <>(mt.getRows());
			rows.sort(comparator::compare);
		}

		List <Row> out;
		if (limit > 0) {
			out = rows.subList(0, Math.min(limit, mt.getNumRow()));
		} else {
			if (offset < 0) {
				offset = 0;
			}
			if (fetch < 0) {
				fetch = mt.getNumRow();
			}

			if (offset == 0 && fetch >= mt.getNumRow()) {
				out = rows;
			} else {
				out = rows.subList(offset, Math.min(offset + fetch, mt.getNumRow()));
			}
		}
		this.setOutputTable(new MTable(out, mt.getSchemaStr()));
	}

}
