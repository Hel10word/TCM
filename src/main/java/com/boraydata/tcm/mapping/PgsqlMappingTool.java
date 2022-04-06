package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** deal with the mapping relationship between PostgreSQL Type and TCM Type
 * design by
 * https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types
 * https://www.postgresql.org/docs/13/datatype.html 
 * @author bufan
 * @data 2021/9/1
 */
public class PgsqlMappingTool implements MappingTool {

    static Map<String, TableCloneManageType> mappingMap = new HashMap<>();
    static {

//        https://www.postgresql.org/docs/13/datatype-numeric.html
        mappingMap.put("SMALLINT", TableCloneManageType.INT16);
        mappingMap.put("INTEGER", TableCloneManageType.INT32);
        mappingMap.put("BIGINT", TableCloneManageType.INT64);
        mappingMap.put("DECIMAL", TableCloneManageType.DECIMAL);
        mappingMap.put("NUMERIC", TableCloneManageType.DECIMAL);
        mappingMap.put("REAL", TableCloneManageType.FLOAT32);
        mappingMap.put("DOUBLE PRECISION", TableCloneManageType.FLOAT64);
        mappingMap.put("SMALLSERIAL", TableCloneManageType.INT16);
        mappingMap.put("SERIAL", TableCloneManageType.INT32);
        mappingMap.put("BIGSERIAL", TableCloneManageType.INT64);

//        https://www.postgresql.org/docs/13/datatype-money.html
        mappingMap.put("MONEY", TableCloneManageType.MONEY);

//        https://www.postgresql.org/docs/13/datatype-character.html
        mappingMap.put("CHARACTER VARYING", TableCloneManageType.STRING);
        mappingMap.put("VARCHAR", TableCloneManageType.STRING);
        mappingMap.put("CHARACTER", TableCloneManageType.STRING);
        mappingMap.put("CHAR", TableCloneManageType.STRING);
        mappingMap.put("TEXT", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-binary.html
        mappingMap.put("BYTEA", TableCloneManageType.BYTES);

//        https://www.postgresql.org/docs/13/datatype-datetime.html
        mappingMap.put("TIMESTAMP", TableCloneManageType.TIMESTAMP);
        mappingMap.put("TIMESTAMP WITH TIME ZONE", TableCloneManageType.TIMESTAMP);
        mappingMap.put("TIMESTAMP WITHOUT TIME ZONE", TableCloneManageType.TIMESTAMP);
        mappingMap.put("DATE", TableCloneManageType.DATE);
        mappingMap.put("TIME", TableCloneManageType.TIME);
        mappingMap.put("TIME WITH TIME ZONE", TableCloneManageType.TIME);
        mappingMap.put("TIME WITHOUT TIME ZONE", TableCloneManageType.TIME);
        mappingMap.put("INTERVAL", TableCloneManageType.STRING);

//        https://www.postgresql.org/docs/13/datatype-boolean.html
        mappingMap.put("BOOLEAN", TableCloneManageType.BOOLEAN);

//        https://www.postgresql.org/docs/13/datatype-enum.html
        // unable support

//        https://www.postgresql.org/docs/13/datatype-geometric.html
        mappingMap.put("POINT", TableCloneManageType.TEXT);
        mappingMap.put("LINE", TableCloneManageType.TEXT);
        mappingMap.put("LSEG", TableCloneManageType.TEXT);
        mappingMap.put("BOX", TableCloneManageType.TEXT);
        mappingMap.put("PATH", TableCloneManageType.TEXT);
        mappingMap.put("POLYGON", TableCloneManageType.TEXT);
        mappingMap.put("CIRCLE", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-net-types.html
        mappingMap.put("CIDR", TableCloneManageType.TEXT);
        mappingMap.put("INET", TableCloneManageType.TEXT);
        mappingMap.put("MACADDR", TableCloneManageType.TEXT);
        mappingMap.put("MACADDR8", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-bit.html
        mappingMap.put("BIT", TableCloneManageType.BYTES);
        mappingMap.put("BIT VARYING", TableCloneManageType.BYTES);

//        https://www.postgresql.org/docs/13/datatype-textsearch.html
        mappingMap.put("TSVECTOR", TableCloneManageType.TEXT);
        mappingMap.put("TSQUERY", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-uuid.html
        mappingMap.put("UUID", TableCloneManageType.STRING);

//        https://www.postgresql.org/docs/13/datatype-xml.html
        mappingMap.put("XML", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-json.html
        mappingMap.put("JSON", TableCloneManageType.TEXT);
        mappingMap.put("JSONB", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/arrays.html
        mappingMap.put("ARRAY", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/rowtypes.html
        // unable support


//        https://www.postgresql.org/docs/13/rangetypes.html
        mappingMap.put("INT4RANGE", TableCloneManageType.TEXT);
        mappingMap.put("INT8RANGE", TableCloneManageType.TEXT);
        mappingMap.put("NUMRANGE", TableCloneManageType.TEXT);
        mappingMap.put("TSRANGE", TableCloneManageType.TEXT);
        mappingMap.put("TSTZRANGE", TableCloneManageType.TEXT);
        mappingMap.put("DATERANGE", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/domains.html
        mappingMap.put("OID", TableCloneManageType.TEXT);
        mappingMap.put("REGCLASS", TableCloneManageType.TEXT);
        mappingMap.put("REGCOLLATION", TableCloneManageType.TEXT);
        mappingMap.put("REGCONFIG", TableCloneManageType.TEXT);
        mappingMap.put("REGDICTIONARY", TableCloneManageType.TEXT);
        mappingMap.put("REGNAMESPACE", TableCloneManageType.TEXT);
        mappingMap.put("REGOPER", TableCloneManageType.TEXT);
        mappingMap.put("REGOPERATOR", TableCloneManageType.TEXT);
        mappingMap.put("REGPROC", TableCloneManageType.TEXT);
        mappingMap.put("REGPROCEDURE", TableCloneManageType.TEXT);
        mappingMap.put("REGROLE", TableCloneManageType.TEXT);
        mappingMap.put("REGTYPE", TableCloneManageType.TEXT);


//        https://www.postgresql.org/docs/13/datatype-oid.html
        mappingMap.put("PG_LSN", TableCloneManageType.TEXT);

//        https://www.postgresql.org/docs/13/datatype-pg-lsn.html
        // unable support

    }


    /**
     *  To look up metadata from the mapping ‘mappingMap’ to the TCM datatype
     * @Param null : table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=null}}
     * @Return: null : table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=INT32}}
     */
    @Override
    public Table createSourceMappingTable(Table table) {
        if(table.getColumns() == null || table.getColumns().isEmpty())
            throw new TCMException("Table.getColumns() is null or empty \n"+table.getTableInfo());
        List<Column> columns = table.getColumns();
        for (Column column : columns){
            if (StringUtil.isNullOrEmpty(column.getDataType()))
                throw new TCMException("not found DataType value in "+column.getColumnInfo());
            TableCloneManageType relation = StringUtil.findRelation(mappingMap,column.getDataType(),null);
            if (relation == null){
                    throw new TCMException("not found DataType relation in "+column.getColumnInfo());
            }
            String colDataType = StringUtil.dataTypeFormat(column.getDataType());
            if(relation != null && "MONEY".equalsIgnoreCase(colDataType))
                column.setNumericPrecisionM(65).setNumericPrecisionD(2);
            if(relation != null && "BOOLEAN".equalsIgnoreCase(colDataType))
                column.setCharMaxLength(1L);
            if(relation != null && "INTERVAL".equalsIgnoreCase(colDataType))
                column.setCharMaxLength(255L);
            if(relation != null && "UUID".equalsIgnoreCase(colDataType))
                column.setCharMaxLength(255L);
//            if(relation != null && "ARRAY".equalsIgnoreCase(colDataType))
//                column.setDataType(column.getUdtType());

            column.setTableCloneManageType(relation);
        }
        table.setColumns(columns);
        if(table.getDataSourceType() == null)
            table.setDataSourceType(DataSourceType.POSTGRES);
        if(table.getSourceType() == null)
            table.setSourceType(DataSourceType.POSTGRES);
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
        if(table.getDataSourceType() != null && DataSourceType.POSTGRES.equals(table.getDataSourceType()))
            return cloneTable;
        for (Column col : cloneTable.getColumns())
            col.setDataType(col.getTableCloneManageType().getOutDataType(DataSourceType.POSTGRES));
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
        if(table.getTableName() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceType().name());
        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists ");
        if(table.getSchemaName() == null)
            stringBuilder.append(table.getTableName()).append("(\n");
        else
            stringBuilder.append(table.getSchemaName()).append(".").append(table.getTableName()).append("(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            if(column.getDataType() == null)
                throw new TCMException("Create Table SQL is fail,Because unable use null datatype:"+column.getColumnInfo());
            String colDataType = StringUtil.dataTypeFormat(column.getDataType());
            stringBuilder.append(column.getColumnName()).append(" ");
            if (
                    "BIT".equalsIgnoreCase(colDataType) ||
                    "BIT VARYING".equalsIgnoreCase(colDataType) ||
                    "BYTEA".equalsIgnoreCase(colDataType) ||
                    "CHARACTER".equalsIgnoreCase(colDataType) ||
                    "CHARACTER VARYING".equalsIgnoreCase(colDataType) ||
                    "VARCHAR".equalsIgnoreCase(colDataType) ||
                    "CHAR".equalsIgnoreCase(colDataType)
            ){
                stringBuilder.append(colDataType);
                if(column.getCharMaxLength() != null && column.getCharMaxLength() > 0)
                    stringBuilder.append("(").append(column.getCharMaxLength()).append(")");
            }else if (
                    "NUMERIC".equalsIgnoreCase(colDataType) ||
                    "DECIMAL".equalsIgnoreCase(colDataType)
            ){
                stringBuilder.append(colDataType);
                if(column.getNumericPrecisionM() != null && column.getNumericPrecisionM() > 0){
                    stringBuilder.append("(").append(column.getNumericPrecisionM());
                    if(column.getNumericPrecisionD() != null &&column.getNumericPrecisionD() > 0)
                        stringBuilder.append(",").append(column.getNumericPrecisionD());
                    stringBuilder.append(")");
                }
            }else if ("ARRAY".equalsIgnoreCase(colDataType)){
                if(column.getUdtType() != null)
                    colDataType = column.getUdtType();
                stringBuilder.append(colDataType);
            }else if (
                    "TIMESTAMP".equalsIgnoreCase(colDataType) ||
                    "TIMESTAMP WITHOUT TIME ZONE".equalsIgnoreCase(colDataType) ||
                    "TIMESTAMP WITH TIME ZONE".equalsIgnoreCase(colDataType) ||
                    "TIME".equalsIgnoreCase(colDataType) ||
                    "TIME WITHOUT TIME ZONE".equalsIgnoreCase(colDataType) ||
                    "TIME WITH TIME ZONE".equalsIgnoreCase(colDataType)
            ){
                if(column.getDatetimePrecision() != null && column.getDatetimePrecision() >= 0 && column.getDatetimePrecision() <= 6){
                    String fsp = "("+column.getDatetimePrecision()+")";
                    if(!colDataType.contains(" "))
                        colDataType += fsp;
                    else
                        colDataType = colDataType.replaceFirst(" ",fsp+" ");
                }
                stringBuilder.append(colDataType);
            }else
                stringBuilder.append(colDataType);

             if (Boolean.FALSE.equals(column.getNullAble()))
                stringBuilder.append(" NOT NULL");

            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(");");
        return stringBuilder.toString();
    }
}
