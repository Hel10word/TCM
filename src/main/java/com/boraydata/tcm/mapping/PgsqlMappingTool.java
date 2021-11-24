package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;
import com.sun.prism.PixelFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** deal with the mapping relationship between PostgreSQL Type and TCM Type
 * design by https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types
 * @author bufan
 * @data 2021/9/1
 */
public class PgsqlMappingTool implements MappingTool {

    static Map<String, TableCloneManageType> mappingMap = new HashMap<>();
    static {
        mappingMap.put("BOOLEAN", TableCloneManageType.BOOLEAN);
        mappingMap.put("SMALLINT", TableCloneManageType.INT16);
        mappingMap.put("SMALLSERIAL", TableCloneManageType.INT16);
        mappingMap.put("INTEGER", TableCloneManageType.INT32);
        mappingMap.put("SERIAL", TableCloneManageType.INT32);
        mappingMap.put("BIGINT", TableCloneManageType.INT64);
        mappingMap.put("BIGSERIAL", TableCloneManageType.INT64);
        mappingMap.put("REAL", TableCloneManageType.FLOAT32);
        mappingMap.put("DOUBLE", TableCloneManageType.FLOAT64);
        mappingMap.put("PRECISION", TableCloneManageType.FLOAT64);
        mappingMap.put("CHAR", TableCloneManageType.STRING);
        mappingMap.put("VARCHAR", TableCloneManageType.STRING);
        mappingMap.put("CHARACTER", TableCloneManageType.STRING);
        mappingMap.put("CHARACTERVARYING", TableCloneManageType.STRING);

        /*
         * TIME(1) and TIME(6) is not distinguished, so default is INT64;
         * */
        mappingMap.put("TIMESTAMP", TableCloneManageType.TIMESTAMP);
        mappingMap.put("TIMESTAMPWITHTIMEZONE", TableCloneManageType.TIMESTAMP);
        mappingMap.put("TIMESTAMPWITHOUTTIMEZONE", TableCloneManageType.TIMESTAMP);


        mappingMap.put("TIME", TableCloneManageType.TIME);
        mappingMap.put("TIMEWITHTIMEZONE", TableCloneManageType.TIME);
        mappingMap.put("TIMEWITHOUTTIMEZONE", TableCloneManageType.TIME);

        mappingMap.put("INTERVAL", TableCloneManageType.STRING);

        mappingMap.put("BYTEA", TableCloneManageType.BYTES);
        mappingMap.put("JSON", TableCloneManageType.TEXT);
        mappingMap.put("JSONB", TableCloneManageType.TEXT);
        mappingMap.put("XML", TableCloneManageType.TEXT);
        mappingMap.put("UUID", TableCloneManageType.STRING);
        mappingMap.put("POINT", TableCloneManageType.STRING);
        mappingMap.put("LTREE", TableCloneManageType.STRING);
        mappingMap.put("CITEXT", TableCloneManageType.STRING);
        mappingMap.put("INET", TableCloneManageType.STRING);
        mappingMap.put("INT4RANGE", TableCloneManageType.STRING);
        mappingMap.put("INT8RANGE", TableCloneManageType.STRING);
        mappingMap.put("NUMRANGE", TableCloneManageType.STRING);
        mappingMap.put("TSRANGE", TableCloneManageType.STRING);
        mappingMap.put("TSTZRANGE", TableCloneManageType.STRING);
        mappingMap.put("DATERANGE", TableCloneManageType.STRING);
        mappingMap.put("ENUM", TableCloneManageType.STRING);

        mappingMap.put("DATE", TableCloneManageType.DATE);



        /*
        * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#decimal-values
        * */
        mappingMap.put("NUMERIC", TableCloneManageType.DECIMAL);
        mappingMap.put("DECIMAL", TableCloneManageType.DECIMAL);


        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#hstore-values
         * */
        mappingMap.put("HSTORE", TableCloneManageType.STRING);

        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#network-address-types
         * */
        mappingMap.put("INET", TableCloneManageType.STRING);
        mappingMap.put("CIDR", TableCloneManageType.STRING);
        mappingMap.put("MACADDR", TableCloneManageType.STRING);
        mappingMap.put("MACADDR8", TableCloneManageType.STRING);

        /*
         * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#_postgis_types
         * */
        mappingMap.put("GEOMETRY", TableCloneManageType.STRUCT);


        /*
         * The following are self-defined
         * If have other data types that need to be mapped, please define below
         * */
        mappingMap.put("BIT", TableCloneManageType.BYTES);
        mappingMap.put("BOX", TableCloneManageType.STRUCT);
        mappingMap.put("CIRCLE", TableCloneManageType.STRUCT);
        mappingMap.put("DOUBLEPRECISION", TableCloneManageType.FLOAT64);
        mappingMap.put("LINE", TableCloneManageType.STRUCT);
        mappingMap.put("LSEG", TableCloneManageType.STRUCT);
        mappingMap.put("MONEY", TableCloneManageType.MONEY);
        mappingMap.put("PATH", TableCloneManageType.STRUCT);
        mappingMap.put("TEXT", TableCloneManageType.TEXT);
        mappingMap.put("POLYGON", TableCloneManageType.STRING);

        mappingMap.put("TSQUERY", TableCloneManageType.TEXT);
        mappingMap.put("TSVECTOR", TableCloneManageType.TEXT);
        mappingMap.put("TXID_SNAPSHOT", TableCloneManageType.STRUCT);

        mappingMap.put("BITVARYING", TableCloneManageType.BYTES);

        mappingMap.put("ARRAY", TableCloneManageType.TEXT);
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
            TableCloneManageType relation = StringUtil.findRelation(mappingMap,column.getDataType(),null);
//            if (relation == null){
//                    throw new TCMException("not found DataType relation in "+column.getColumnInfo());
//            }
            column.setTableCloneManageType(relation);
            if(relation.equals(TableCloneManageType.MONEY))
                column.setNumericPrecisionM(65).setNumericPrecisionD(2);
            if(relation.equals(TableCloneManageType.BOOLEAN))
                column.setCharMaxLength(1L);
        }
        table.setColumns(columns);
        return table;
    }


    /**
     * return table provides the mapping table to the PgSQL data type, according "*.core.TableCloneManageType"
     * @Param null :    table={...,MYSQL,..,columns={...,...,col_int,DataType=mediumint,dataTypeMapping=INT32}}
     * @Return: null :  table={null,PGSQL,..,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     */
//    @Override
//    public Table createCloneMappingTable(Table table) {
//        return createCloneMappingTable(table,table.getTablename());
//    }

    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        for (Column col : cloneTable.getColumns()){
            col.setDataType(col.getTableCloneManageType().getOutDataType(DataSourceType.POSTGRES));
        }
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
//            if (Arrays.binarySearch(new String[]{"bin","bit varying","character","character varying"},column.getDataType()) >= 0)
            if (
                    "bit".equalsIgnoreCase(column.getDataType()) ||
                    "bit varying".equalsIgnoreCase(column.getDataType()) ||
                    "character".equalsIgnoreCase(column.getDataType()) ||
                    "character varying".equalsIgnoreCase(column.getDataType())
            )
                stringBuilder.append("("+column.getCharMaxLength()+")");
            if ("numeric".equalsIgnoreCase(column.getDataType())){
                if(column.getNumericPrecisionM()>0){
                    stringBuilder.append("("+column.getNumericPrecisionM());
                    if(column.getNumericPrecisionD()>0)
                        stringBuilder.append(","+column.getNumericPrecisionD());
                    stringBuilder.append(")");

                }
            }

            if (Boolean.FALSE.equals(column.getNullAble()))
                stringBuilder.append(" NOT NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(");");
        return stringBuilder.toString();
    }
}
