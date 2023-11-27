package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvTypeConverter;
import com.alibaba.alink.operator.common.io.csv.InternalCsvSourceBatchOp;
import com.alibaba.alink.params.io.CsvSourceParams;

/**
 * Data source of a CSV (Comma Separated Values) file.
 * csv文件处理
 *
 * <p>
 * The file can reside in places including:
 * <p><ul>
 * <li> local file system
 * <li> hdfs
 * <li> http
 * </ul></p>
 */
@IoOpAnnotation(name = "csv", ioType = IOType.SourceBatch)
@NameCn("CSV文件读入")
public class CsvSourceBatchOp extends BaseSourceBatchOp <CsvSourceBatchOp>
	implements CsvSourceParams <CsvSourceBatchOp> {

	public CsvSourceBatchOp() {
		this(new Params());
	}

	public CsvSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(CsvSourceBatchOp.class), params);
	}

	public CsvSourceBatchOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, schemaStr)
		);
	}

	public CsvSourceBatchOp(String filePath, TableSchema schema) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(schema))
		);
	}

	public CsvSourceBatchOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes,
							String fieldDelim, String rowDelim) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
			.set(FIELD_DELIMITER, fieldDelim)
			.set(ROW_DELIMITER, rowDelim)
		);
	}

	/**
	 * 初始化数据源
	 *
	 * @return
	 */
	@Override
	protected Table initializeDataSource() {
		// 根据传入的schema， 生成TableSchema, 这里注意 TableSchema 实际flink中的对象
		TableSchema schema = TableUtil.schemaStr2Schema(getSchemaStr());
		// 列名称
		String[] colNames = schema.getFieldNames();
		// 列类型
		TypeInformation <?>[] colTypes = schema.getFieldTypes();

		// 设置 SCHEMA_STR参数
		Params rawCsvParams = getParams().clone()
			.set(
				CsvSourceParams.SCHEMA_STR,
				// 将schema转为string 类型
				TableUtil.schema2SchemaStr(new TableSchema(colNames, CsvTypeConverter.rewriteColTypes(colTypes)))
			);

		BatchOperator <?> source = new InternalCsvSourceBatchOp(rawCsvParams);

		// 张量处理
		source = CsvTypeConverter.toTensorPipelineModel(getParams(), colNames, colTypes).transform(source);
		// 向量处理
		source = CsvTypeConverter.toVectorPipelineModel(getParams(), colNames, colTypes).transform(source);
		source = CsvTypeConverter.toMTablePipelineModel(getParams(), colNames, colTypes).transform(source);

		return source.getOutputTable();
	}
}
