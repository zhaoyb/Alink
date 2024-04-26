package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.sql.SelectParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * todo:
 * 1. not support count(1), count(*) ...; it will to multi line.
 */
public class SelectMapper extends Mapper {

	private Mapper[] mappers;

	private String[] outColNames;

	private TypeInformation <?>[] outColTypes;

	public SelectMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		String[] colNames = this.getDataSchema().getFieldNames();
		String clause = params.get(SelectParams.CLAUSE);
		clause = SelectUtils.convertRegexClause2ColNames(colNames, clause);

		if (SelectUtils.isSimpleSelect(clause, colNames)) {
			mappers = new Mapper[1];
			mappers[0] = new SimpleSelectMapper(this.getDataSchema(), new Params().set(SelectParams.CLAUSE, clause));
			mappers[0].open();
			TableSchema outSchema = mappers[0].getOutputSchema();
			this.outColNames = outSchema.getFieldNames();
			this.outColTypes = outSchema.getFieldTypes();
		} else {
			Tuple2 <String, Boolean>[] clauseSplits = SelectUtils.splitClauseBySimpleClause(clause, colNames);
			mappers = new Mapper[clauseSplits.length];
			List <String> outColNames = new ArrayList <>();
			List <TypeInformation <?>> outColTypes = new ArrayList <>();
			for (int i = 0; i < clauseSplits.length; i++) {
				String curClause = clauseSplits[i].f0;
				Params curParams = new Params().set(SelectParams.CLAUSE, curClause);
				if (SelectUtils.isSimpleSelect(curClause, colNames)) {
					mappers[i] = new SimpleSelectMapper(this.getDataSchema(), curParams);
				} else {
					mappers[i] = new CalciteSelectMapper(this.getDataSchema(), curParams);
				}
				mappers[i].open();
				TableSchema outSchema = mappers[i].getOutputSchema();
				outColNames.addAll(Arrays.asList(outSchema.getFieldNames()));
				outColTypes.addAll(Arrays.asList(outSchema.getFieldTypes()));
			}
			this.outColNames = outColNames.toArray(new String[0]);
			this.outColTypes = outColTypes.toArray(new TypeInformation <?>[0]);
		}
	}

	@Override
	public void close() {
		if (mappers != null) {
			for (Mapper mapper : mappers) {
				if (mapper != null) {
					mapper.close();
				}
			}
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
	}

	@Override
	public Row map(Row row) throws Exception {
		if (mappers.length == 0) {
			return null;
		} else if (mappers.length == 1) {
			return mappers[0].map(row);
		} else {
			Row outRow = new Row(outColNames.length);
			int idx = 0;
			for (Mapper mapper : mappers) {
				Row r = mapper.map(row);
				for (int j = 0; j < r.getArity(); j++) {
					outRow.setField(idx, r.getField(j));
					idx++;
				}
			}
			return outRow;
		}
	}

	@Override
	public void bufferMap(Row bufferRow, int[] bufferSelectedColIndices, int[] bufferResultColIndices)
		throws Exception {
		if (mappers.length == 0) {
			return;
		} else if (mappers.length == 1) {
			mappers[0].bufferMap(bufferRow, bufferSelectedColIndices, bufferResultColIndices);
		} else {
			Row in = Row.project(bufferRow, bufferSelectedColIndices);
			Row out = map(in);
			for (int i = 0; i < bufferResultColIndices.length; i++) {
				bufferRow.setField(bufferResultColIndices[i], out.getField(i));
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String clause = params.get(SelectParams.CLAUSE);
		String[] colNames = dataSchema.getFieldNames();
		String newClause = SelectUtils.convertRegexClause2ColNames(colNames, clause);
		if (SelectUtils.isSimpleSelect(newClause, colNames)) {
			return SimpleSelectMapper.prepareIoSchemaImpl(dataSchema, new Params().set(SelectParams.CLAUSE,
				newClause));
		} else {
			Tuple2 <String, Boolean>[] clauseSplits = SelectUtils.splitClauseBySimpleClause(newClause, colNames);
			mappers = new Mapper[clauseSplits.length];
			List <String> outColNames = new ArrayList <>();
			List <TypeInformation <?>> outColTypes = new ArrayList <>();
			for (int i = 0; i < clauseSplits.length; i++) {
				String curClause = clauseSplits[i].f0;
				Params curParams = new Params().set(SelectParams.CLAUSE, curClause);
				if (SelectUtils.isSimpleSelect(curClause, colNames)) {
					mappers[i] = new SimpleSelectMapper(this.getDataSchema(), curParams);
				} else {
					mappers[i] = new CalciteSelectMapper(this.getDataSchema(), curParams);
				}
				mappers[i].open();
				TableSchema outSchema = mappers[i].getOutputSchema();
				outColNames.addAll(Arrays.asList(outSchema.getFieldNames()));
				outColTypes.addAll(Arrays.asList(outSchema.getFieldTypes()));
				mappers[i].close();
			}
			this.outColNames = outColNames.toArray(new String[0]);
			this.outColTypes = outColTypes.toArray(new TypeInformation <?>[0]);
			return Tuple4.of(
				dataSchema.getFieldNames(),
				this.outColNames,
				this.outColTypes,
				new String[0]
			);
		}
	}

	//over write output schema, output cols order by clause, otherwise it will order by input schema,
	@Override
	public TableSchema getOutputSchema() {
		if (this.outColNames == null || this.outColTypes == null) {
			Tuple4 <String[], String[], TypeInformation <?>[], String[]> t4 = prepareIoSchema(this.getDataSchema(),
				this.params);
			this.outColNames = t4.f1;
			this.outColTypes = t4.f2;
		}

		return new TableSchema(this.outColNames, this.outColTypes);
	}

}
