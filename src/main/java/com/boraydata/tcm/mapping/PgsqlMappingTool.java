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

    static Map<String, DataTypeMapping> mappingMap = new HashMap<>();
    static {
        mappingMap.put("BOOLEAN",DataTypeMapping.BOOLEAN);
        mappingMap.put("BIT(1)",DataTypeMapping.BOOLEAN);
        mappingMap.put("BIT(>1)",DataTypeMapping.BYTES);
        mappingMap.put("SMALLINT",DataTypeMapping.INT16);
        mappingMap.put("SMALLSERIAL",DataTypeMapping.INT16);
        mappingMap.put("INTEGER",DataTypeMapping.INT32);
        mappingMap.put("SERIAL",DataTypeMapping.INT32);
        mappingMap.put("BIGINT",DataTypeMapping.INT64);
        mappingMap.put("BIGSERIAL",DataTypeMapping.INT64);
        mappingMap.put("REAL",DataTypeMapping.FLOAT32);
        mappingMap.put("REAL",DataTypeMapping.FLOAT32);
        mappingMap.put("DOUBLE",DataTypeMapping.FLOAT64);
        mappingMap.put("PRECISION",DataTypeMapping.FLOAT64);
        mappingMap.put("CHAR",DataTypeMapping.STRING);
        mappingMap.put("VARCHAR",DataTypeMapping.STRING);
        mappingMap.put("CHARACTER",DataTypeMapping.STRING);
        mappingMap.put("CHARACTERVARYING",DataTypeMapping.STRING);
        mappingMap.put("TIMESTAMPTZ",DataTypeMapping.STRING);
        mappingMap.put("TIMESTAMPWITHTIMEZONE",DataTypeMapping.STRING);
        mappingMap.put("TIMETZ",DataTypeMapping.STRING);
        mappingMap.put("TIMEWITHTIMEZONE",DataTypeMapping.STRING);

        mappingMap.put("INTERVAL",DataTypeMapping.INT64);

        mappingMap.put("BYTEA",DataTypeMapping.BYTES);
        mappingMap.put("JSON",DataTypeMapping.STRING);
        mappingMap.put("JSONB",DataTypeMapping.STRING);
        mappingMap.put("XML",DataTypeMapping.STRING);
        mappingMap.put("UUID",DataTypeMapping.STRING);
        mappingMap.put("POINT",DataTypeMapping.STRING);
        mappingMap.put("LTREE",DataTypeMapping.STRING);
        mappingMap.put("CITEXT",DataTypeMapping.STRING);
        mappingMap.put("INET",DataTypeMapping.STRING);
        mappingMap.put("INT4RANGE",DataTypeMapping.STRING);
        mappingMap.put("INT8RANGE",DataTypeMapping.STRING);
        mappingMap.put("NUMRANGE",DataTypeMapping.STRING);
        mappingMap.put("TSRANGE",DataTypeMapping.STRING);
        mappingMap.put("TSTZRANGE",DataTypeMapping.STRING);
        mappingMap.put("DATERANGE",DataTypeMapping.STRING);
        mappingMap.put("ENUM",DataTypeMapping.STRING);

        mappingMap.put("DATE",DataTypeMapping.INT32);

        /*
         * TIME(1) and TIME(6) is not distinguished, so default is INT64;
         * */
        mappingMap.put("TIME",DataTypeMapping.INT64);
        mappingMap.put("TIMESTAMP",DataTypeMapping.INT64);

        /*
        * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#decimal-values
        * */
        mappingMap.put("NUMERIC",DataTypeMapping.BYTES);
        mappingMap.put("DECIMAL",DataTypeMapping.FLOAT64);


        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#hstore-values
         * */
        mappingMap.put("HSTORE",DataTypeMapping.STRING);

        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#network-address-types
         * */
        mappingMap.put("INET",DataTypeMapping.STRING);
        mappingMap.put("CIDR",DataTypeMapping.STRING);
        mappingMap.put("MACADDR",DataTypeMapping.STRING);
        mappingMap.put("MACADDR8",DataTypeMapping.STRING);

        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#_postgis_types
         * */
        mappingMap.put("GEOMETRY",DataTypeMapping.STRUCT);


        /*
         * The following are self-defined
         * If have other data types that need to be mapped, please define below
         * */
        mappingMap.put("BIT",DataTypeMapping.BYTES);
        mappingMap.put("BOX",DataTypeMapping.STRUCT);
        mappingMap.put("CIRCLE",DataTypeMapping.STRUCT);
        mappingMap.put("DOUBLEPRECISION",DataTypeMapping.FLOAT64);
        mappingMap.put("LINE",DataTypeMapping.STRUCT);
        mappingMap.put("LSEG",DataTypeMapping.STRUCT);
        mappingMap.put("MONEY",DataTypeMapping.FLOAT32);
        mappingMap.put("PATH",DataTypeMapping.STRUCT);
        mappingMap.put("TEXT",DataTypeMapping.STRING);
        mappingMap.put("POLYGON",DataTypeMapping.STRING);
        mappingMap.put("TIMEWITHOUTTIMEZONE",DataTypeMapping.STRING);
        mappingMap.put("TIMESTAMPWITHOUTTIMEZONE",DataTypeMapping.STRING);
        mappingMap.put("TSQUERY",DataTypeMapping.STRUCT);
        mappingMap.put("TSVECTOR",DataTypeMapping.STRUCT);
        mappingMap.put("TXID_SNAPSHOT",DataTypeMapping.STRUCT);

        mappingMap.put("BITVARYING",DataTypeMapping.STRING);
        mappingMap.put("DOUBLEPRECISION",DataTypeMapping.FLOAT64);
    }


    /**
     *  To look up metadata from the mapping ‘mappingMap’ to the TCM datatype
     * @Param null : table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=null}}
     * @Return: null : table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=INT32}}
     */
    @Override
    public Table createSourceMappingTable(Table table) {
        List<Column> columns = table.getColumns();
        for (Column column : columns){
            if (StringUtil.isNullOrEmpty(column.getDataType()))
                throw new TCMException("not found DataType value in "+column.getColumnInfo());
            DataTypeMapping relation = StringUtil.findRelation(mappingMap,column.getDataType(),null);
//            if (relation == null){
//                    throw new TCMException("not found DataType relation in "+column.getColumnInfo());
//            }
            column.setDataTypeMapping(relation);
        }
        table.setColumns(columns);
        return table;
    }


    /**
     * return table provides the mapping table to the PgSQL data type, according "*.core.DataTypeMapping"
     * @Param null :    table={...,MYSQL,..,columns={...,...,col_int,DataType=mediumint,dataTypeMapping=INT32}}
     * @Return: null :  table={null,PGSQL,..,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     */
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



    /**
     *
     * generate the same information table creation SQL.
     *
     * Please refer to the official documentation (PgSQL 13)
     *  url: https://www.postgresql.org/docs/13/sql-createtable.html
     *
     * in pgsql,when you create a new table,you can provide SchemaName or not.
     *  You should view the ({url} # Description),and make some change.
     *
     * @Param Table : table={null,PGSQL,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String : "Create Table If Not Exists test_table(col_int int);"
     */
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
                throw new TCMException("Create Table SQL is fail,Because unable use null datatype:"+column.getColumnInfo());
            stringBuilder.append(column.getColumnName()).append(" ").append(column.getDataType());
            if (Boolean.FALSE.equals(column.isNullAble()))
                stringBuilder.append(" NOT NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(");");
        return stringBuilder.toString();
    }
}
