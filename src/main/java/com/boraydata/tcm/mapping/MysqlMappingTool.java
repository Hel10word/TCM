package com.boraydata.tcm.mapping;


import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

import java.util.*;

/** deal with the mapping relationship between Mysql Type and TCM Type
 * design by :
 * https://debezium.io/documentation/reference/1.0/connectors/mysql.html#how-the-mysql-connector-maps-data-types_cdc
 * https://dev.mysql.com/doc/refman/5.7/en/data-types.html
 * @author bufan
 * @data 2021/8/31
 */
public class MysqlMappingTool implements MappingTool {

    static Map<String, TableCloneManageType> mappingMap = new HashMap<>();
    static {
// https://dev.mysql.com/doc/refman/5.7/en/numeric-types.html
        mappingMap.put("INTEGER", TableCloneManageType.INT32);
        mappingMap.put("SMALLINT", TableCloneManageType.INT16);
        mappingMap.put("DECIMAL", TableCloneManageType.DECIMAL);
        mappingMap.put("NUMERIC", TableCloneManageType.DECIMAL);
        mappingMap.put("FLOAT", TableCloneManageType.FLOAT64);
        mappingMap.put("REAL", TableCloneManageType.FLOAT64);
        mappingMap.put("DOUBLE", TableCloneManageType.FLOAT64);
        mappingMap.put("INT", TableCloneManageType.INT32);
        mappingMap.put("BIT", TableCloneManageType.BYTES);
        mappingMap.put("TINYINT(1)", TableCloneManageType.BOOLEAN);
        mappingMap.put("TINYINT", TableCloneManageType.INT8);
        mappingMap.put("MEDIUMINT", TableCloneManageType.INT32);
        mappingMap.put("BIGINT", TableCloneManageType.INT64);
        mappingMap.put("BOOL", TableCloneManageType.BOOLEAN);
        mappingMap.put("BOOLEAN", TableCloneManageType.BOOLEAN);

// https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html
        mappingMap.put("DATE", TableCloneManageType.DATE);
        mappingMap.put("TIME", TableCloneManageType.TIME);
        mappingMap.put("DATETIME", TableCloneManageType.TIMESTAMP);
        mappingMap.put("TIMESTAMP", TableCloneManageType.TIMESTAMP);
        mappingMap.put("YEAR", TableCloneManageType.INT32);


// https://dev.mysql.com/doc/refman/5.7/en/string-types.html
        mappingMap.put("CHAR", TableCloneManageType.STRING);
        mappingMap.put("VARCHAR", TableCloneManageType.STRING);
        mappingMap.put("BINARY", TableCloneManageType.BYTES);
        mappingMap.put("VARBINARY", TableCloneManageType.BYTES);
        mappingMap.put("BLOB", TableCloneManageType.BYTES);
        mappingMap.put("TEXT", TableCloneManageType.STRING);
        mappingMap.put("TINYBLOB", TableCloneManageType.BYTES);
        mappingMap.put("TINYTEXT", TableCloneManageType.STRING);
        mappingMap.put("MEDIUMBLOB", TableCloneManageType.BYTES);
        mappingMap.put("MEDIUMTEXT", TableCloneManageType.STRING);
        mappingMap.put("LONGBLOB", TableCloneManageType.BYTES);
        mappingMap.put("LONGTEXT", TableCloneManageType.STRING);
        mappingMap.put("ENUM", TableCloneManageType.STRING);
        mappingMap.put("SET", TableCloneManageType.STRING);


// https://dev.mysql.com/doc/refman/5.7/en/spatial-types.html
        mappingMap.put("GEOMETRY", TableCloneManageType.TEXT);
        mappingMap.put("POINT", TableCloneManageType.TEXT);
        mappingMap.put("LINESTRING", TableCloneManageType.TEXT);
        mappingMap.put("POLYGON", TableCloneManageType.TEXT);
        mappingMap.put("MULTIPOINT", TableCloneManageType.TEXT);
        mappingMap.put("MULTILINESTRING", TableCloneManageType.TEXT);
        mappingMap.put("MULTIPOLYGON", TableCloneManageType.TEXT);
        mappingMap.put("GEOMETRYCOLLECTION", TableCloneManageType.TEXT);

// https://dev.mysql.com/doc/refman/5.7/en/json.html
        mappingMap.put("JSON", TableCloneManageType.TEXT);
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
//            if (relation == null)
//                throw new TCMException("not found DataType relation in "+column.getColumnInfo());
            column.setTableCloneManageType(relation);
        }
        return table;
    }


    /**
     * return table provides the mapping table to the PgSQL data type, according "*.core.TableCloneManageType"
     * @Param null :    table={...,PGSQL,..,columns={...,...,col_int,DataType=serial,dataTypeMapping=INT32}}
     * @Return: null :  table={null,MYSQL,..,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     */
//    @Override
//    public Table createCloneMappingTable(Table table, String tableName) {
//        return createCloneMappingTable(table,table.getTablename());
//    }

    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        for (Column col : cloneTable.getColumns())
            col.setDataType(col.getTableCloneManageType().getOutDataType(DataSourceType.MYSQL));
        cloneTable.setDataSourceType(DataSourceType.MYSQL);
        return cloneTable;
    }

    /**
     *
     * generate the same information table creation SQL.
     *
     * Please refer to the official documentation (MySQL 5.7)
     *  url: https://dev.mysql.com/doc/refman/5.7/en/create-table.html
     *
     *  "Engine InnoDB" is added by default, you can also not add;
     *
     * @Param Table : table={null,MySQL,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String : "Create Table If Not Exists test_table(col_int int)Engine InnoDB;"
     */
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTablename() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceType().name());
        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists "+table.getTablename()+"(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            if(column.getDataType() == null)
                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column.getColumnInfo());
            String colName = StringUtil.dataTypeFormat(column.getDataType());
            stringBuilder.append(column.getColumnName()).append(" ");
            if("DECIMAL".equalsIgnoreCase(colName)){
                stringBuilder.append(colName);
                if(column.getNumericPrecisionM()>0){
                    stringBuilder.append("("+column.getNumericPrecisionM());
                    if(column.getNumericPrecisionD()>0)
                        stringBuilder.append(","+column.getNumericPrecisionD());
                    stringBuilder.append(")");
                }else
                    stringBuilder.append("(65,30)");
            }else if ("VARBINARY".equalsIgnoreCase(colName)){
                stringBuilder.append(colName);
                if(column.getCharMaxLength() > 0)
                    stringBuilder.append("("+column.getCharMaxLength()+")");
                else
                    stringBuilder.append("(1024)");
            }else if ("VARCHAR".equalsIgnoreCase(colName) || "CHAR".equalsIgnoreCase(colName)){
                stringBuilder.append(colName);
                if(column.getCharMaxLength() > 0)
                    stringBuilder.append("("+column.getCharMaxLength()+")");
                else
                    stringBuilder.append("(255)");
            }else if("TIMESTAMP".equalsIgnoreCase(colName)){
                stringBuilder.append(colName);
                String tempStr = "/ DEFAULT CURRENT_TIMESTAMP/ ON UPDATE CURRENT_TIMESTAMP/";
                if(column.getDatetimePrecision()>0)
                    stringBuilder.append(tempStr.replaceAll("/","("+column.getDatetimePrecision()+")"));
                else
                    stringBuilder.append(tempStr.replaceAll("/",""));
            }else if("DATETIME".equalsIgnoreCase(colName)||"TIME".equalsIgnoreCase(colName)){
                stringBuilder.append(colName);
                if(column.getDatetimePrecision()>0)
                    stringBuilder.append("("+column.getDatetimePrecision()+")");
            }else
                stringBuilder.append(column.getDataType());

            if (Boolean.FALSE.equals(column.getNullAble()))
                stringBuilder.append(" not NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(")Engine InnoDB;");
        return stringBuilder.toString();
    }
}
