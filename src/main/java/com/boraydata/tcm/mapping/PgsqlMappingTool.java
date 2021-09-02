package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.DataTypeMapping;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** deal with the mapping relationship between PostgreSQL Type and TCM Type
 * design by https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types
 * @author bufan
 * @data 2021/9/1
 */
public class PgsqlMappingTool implements MappingTool {

    static Map<String, DataTypeMapping> MappingMap = new HashMap<>();
    static {
        MappingMap.put("BOOLEAN",DataTypeMapping.BOOLEAN);
        MappingMap.put("BIT(1)",DataTypeMapping.BOOLEAN);
        MappingMap.put("BIT(>1)",DataTypeMapping.BYTES);
        MappingMap.put("SMALLINT",DataTypeMapping.INT16);
        MappingMap.put("SMALLSERIAL",DataTypeMapping.INT16);
        MappingMap.put("INTEGER",DataTypeMapping.INT32);
        MappingMap.put("SERIAL",DataTypeMapping.INT32);
        MappingMap.put("BIGINT",DataTypeMapping.INT64);
        MappingMap.put("BIGSERIAL",DataTypeMapping.INT64);
        MappingMap.put("REAL",DataTypeMapping.FLOAT32);
        MappingMap.put("REAL",DataTypeMapping.FLOAT32);
        MappingMap.put("DOUBLE",DataTypeMapping.FLOAT64);
        MappingMap.put("PRECISION",DataTypeMapping.FLOAT64);
        MappingMap.put("CHAR",DataTypeMapping.STRING);
        MappingMap.put("VARCHAR",DataTypeMapping.STRING);
        MappingMap.put("CHARACTER",DataTypeMapping.STRING);
        MappingMap.put("CHARACTERVARYING",DataTypeMapping.STRING);
        MappingMap.put("TIMESTAMPTZ",DataTypeMapping.STRING);
        MappingMap.put("TIMESTAMPWITHTIMEZONE",DataTypeMapping.STRING);
        MappingMap.put("TIMETZ",DataTypeMapping.STRING);
        MappingMap.put("TIMEWITHTIMEZONE",DataTypeMapping.STRING);

        MappingMap.put("INTERVAL",DataTypeMapping.INT64);

        MappingMap.put("BYTEA",DataTypeMapping.BYTES);
        MappingMap.put("JSON",DataTypeMapping.STRING);
        MappingMap.put("JSONB",DataTypeMapping.STRING);
        MappingMap.put("XML",DataTypeMapping.STRING);
        MappingMap.put("UUID",DataTypeMapping.STRING);
        MappingMap.put("POINT",DataTypeMapping.STRING);
        MappingMap.put("LTREE",DataTypeMapping.STRING);
        MappingMap.put("CITEXT",DataTypeMapping.STRING);
        MappingMap.put("INET",DataTypeMapping.STRING);
        MappingMap.put("INT4RANGE",DataTypeMapping.STRING);
        MappingMap.put("INT8RANGE",DataTypeMapping.STRING);
        MappingMap.put("NUMRANGE",DataTypeMapping.STRING);
        MappingMap.put("TSRANGE",DataTypeMapping.STRING);
        MappingMap.put("TSTZRANGE",DataTypeMapping.STRING);
        MappingMap.put("DATERANGE",DataTypeMapping.STRING);
        MappingMap.put("ENUM",DataTypeMapping.STRING);

        MappingMap.put("DATE",DataTypeMapping.INT32);

        // TIME(1) and TIME(6) is not distinguished, so default is INT64;
        MappingMap.put("TIME",DataTypeMapping.INT64);
        MappingMap.put("TIMESTAMP",DataTypeMapping.INT64);

        // https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#decimal-values
        MappingMap.put("NUMERIC",DataTypeMapping.BYTES);
        MappingMap.put("DECIMAL",DataTypeMapping.FLOAT64);

        // https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#hstore-values
        MappingMap.put("HSTORE",DataTypeMapping.STRING);

        // https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#network-address-types
        MappingMap.put("INET",DataTypeMapping.STRING);
        MappingMap.put("CIDR",DataTypeMapping.STRING);
        MappingMap.put("MACADDR",DataTypeMapping.STRING);
        MappingMap.put("MACADDR8",DataTypeMapping.STRING);

        // https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#_postgis_types
        MappingMap.put("GEOMETRY",DataTypeMapping.STRUCT);


        // The following are self-defined
        // If have other data types that need to be mapped, please define below
        MappingMap.put("BIT",DataTypeMapping.BYTES);
        MappingMap.put("BOX",DataTypeMapping.STRUCT);
        MappingMap.put("CIRCLE",DataTypeMapping.STRUCT);
        MappingMap.put("DOUBLEPRECISION",DataTypeMapping.FLOAT64);
        MappingMap.put("LINE",DataTypeMapping.STRUCT);
        MappingMap.put("LSEG",DataTypeMapping.STRUCT);
        MappingMap.put("MONEY",DataTypeMapping.FLOAT32);
        MappingMap.put("PATH",DataTypeMapping.STRUCT);
        MappingMap.put("TEXT",DataTypeMapping.STRING);
        MappingMap.put("POLYGON",DataTypeMapping.STRING);
        MappingMap.put("TIMEWITHOUTTIMEZONE",DataTypeMapping.STRING);
        MappingMap.put("TIMESTAMPWITHOUTTIMEZONE",DataTypeMapping.STRING);
        MappingMap.put("TSQUERY",DataTypeMapping.STRUCT);
        MappingMap.put("TSVECTOR",DataTypeMapping.STRUCT);
        MappingMap.put("TXID_SNAPSHOT",DataTypeMapping.STRUCT);

        MappingMap.put("BITVARYING",DataTypeMapping.STRING);
        MappingMap.put("DOUBLEPRECISION",DataTypeMapping.FLOAT64);
    }


    @Override
    public Table createSourceMappingTable(Table table) {
        List<Column> columns = table.getColumns();
        for (Column column : columns){
            if (StringUtil.isNullOrEmpty(column.getDataType()))
                throw new TCMException("not found DataType value in "+column.getColumnInfo());
            DataTypeMapping relation = StringUtil.findRelation(MappingMap,column.getDataType(),null);
//            if (relation == null)
//                throw new TCMException("not found DataType relation in "+column.getColumnInfo());
            column.setDataTypeMapping(relation);
        }
        table.setColumns(columns);
        return table;
    }


    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        List<Column> sourceCols = cloneTable.getColumns();
        List<Column> cloneCols = new LinkedList<>();
        for (Column col : sourceCols){
            Column c = col.clone();
            c.setDataType(col.getDataTypeMapping().getOutDataType(DataSourceType.POSTGRES));
            cloneCols.add(c);
        }
        cloneTable.setCatalogname(null);
        cloneTable.setSchemaname(null);
        cloneTable.setColumns(cloneCols);
        cloneTable.setDataSourceType(DataSourceType.POSTGRES);
        return cloneTable;
    }


    //  Please refer to the official documentation
    //  https://www.postgresql.org/docs/13/sql-createtable.html
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTablename() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceType().name());
        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists ");
        if(table.getSchemaname() == null)
            stringBuilder.append(table.getTablename()).append("(\n");
        else
            stringBuilder.append(table.getSchemaname()).append(".").append(table.getTablename()).append("(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            if(column.getDataType() == null)
                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column.getColumnInfo());
            stringBuilder.append(column.getColumnName()).append(" ").append(column.getDataType());
            if (Boolean.FALSE.equals(column.isNullAble()))
                stringBuilder.append("NOT NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(");");
        return stringBuilder.toString();
    }
}
