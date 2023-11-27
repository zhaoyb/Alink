package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * The base class of all data sources.
 * 源数据操作基础类
 *
 * @param <T>
 */

@InputPorts()
@OutputPorts(values = {@PortSpec(PortType.ANY)})
public abstract class BaseSourceBatchOp<T extends BaseSourceBatchOp <T>> extends BatchOperator <T> {

	static final IOType IO_TYPE = IOType.SourceBatch;
	private static final long serialVersionUID = -1981109968114443621L;

	protected BaseSourceBatchOp(String nameSrcSnk, Params params) {
		super(params);
		getParams().set(HasIoType.IO_TYPE, IO_TYPE)
			.set(HasIoName.IO_NAME, nameSrcSnk);

	}

	@Override
	public final T linkFrom(BatchOperator <?>... inputs) {
		throw new AkUnsupportedOperationException("Source operator does not support linkFrom()");
	}

	@Override
	public Table getOutputTable() {
		if (isNullOutputTable()) {
			super.setOutputTable(initializeDataSource());
		}
		return super.getOutputTable();
	}

	/**
	 * Initialize the table.
	 */
	protected abstract Table initializeDataSource();
}
