package com.boraydata.tcm.mapping;


import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.DataTypeMapping;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

import java.util.*;

/** deal with the mapping relationship between Mysql Type and TCM Type
 * design by https://debezium.io/documentation/reference/1.0/connectors/mysql.html#how-the-mysql-connector-maps-data-types_cdc
 * @author bufan
 * @data 2021/8/31
 */
public class MysqlMappingTool implements MappingTool {

    static Map<String,DataTypeMapping> mappingMap = new HashMap<>();
    static {
        mappingMap.put("BOOLEAN",DataTypeMapping.BOOLEAN);
        mappingMap.put("BOOL",DataTypeMapping.BOOLEAN);
        mappingMap.put("BIT(1)",DataTypeMapping.BOOLEAN);
        mappingMap.put("BIT(>1)",DataTypeMapping.BYTES);
        mappingMap.put("BIT",DataTypeMapping.BYTES);
        mappingMap.put("TINYINT",DataTypeMapping.INT8);
        mappingMap.put("SMALLINT",DataTypeMapping.INT16);
        mappingMap.put("MEDIUMINT",DataTypeMapping.INT32);
        mappingMap.put("INT",DataTypeMapping.INT32);
        mappingMap.put("INTEGER",DataTypeMapping.INT32);
        mappingMap.put("BIGINT",DataTypeMapping.INT64);
        mappingMap.put("REAL",DataTypeMapping.FLOAT64);
        mappingMap.put("FLOAT",DataTypeMapping.FLOAT64);
        mappingMap.put("DOUBLE",DataTypeMapping.FLOAT64);
        mappingMap.put("CHAR",DataTypeMapping.STRING);
        mappingMap.put("VARCHAR",DataTypeMapping.STRING);
        mappingMap.put("BINARY",DataTypeMapping.BYTES);
        mappingMap.put("VARBINARY",DataTypeMapping.BYTES);
        mappingMap.put("TINYBLOB",DataTypeMapping.BYTES);
        mappingMap.put("TINYTEXT",DataTypeMapping.STRING);
        mappingMap.put("BLOB",DataTypeMapping.BYTES);
        mappingMap.put("TEXT",DataTypeMapping.STRING);
        mappingMap.put("MEDIUMBLOB",DataTypeMapping.BYTES);
        mappingMap.put("MEDIUMTEXT",DataTypeMapping.STRING);
        mappingMap.put("LONGBLOB",DataTypeMapping.BYTES);
        mappingMap.put("LONGTEXT",DataTypeMapping.STRING);
        mappingMap.put("JSON",DataTypeMapping.TEXT);
        mappingMap.put("ENUM",DataTypeMapping.STRING);
        mappingMap.put("SET",DataTypeMapping.STRING);
        // YEAR[(2|4)]
        mappingMap.put("YEAR",DataTypeMapping.INT32);
        mappingMap.put("TIMESTAMP",DataTypeMapping.TIMESTAMP);

        mappingMap.put("DATE",DataTypeMapping.DATE);
        mappingMap.put("TIME",DataTypeMapping.INT64);
        mappingMap.put("DATETIME",DataTypeMapping.INT64);

        // https://debezium.io/documentation/reference/1.0/connectors/mysql.html#_decimal_values
        mappingMap.put("NUMERIC",DataTypeMapping.BYTES);
        mappingMap.put("DECIMAL",DataTypeMapping.DECIMAL);

        // https://debezium.io/documentation/reference/1.0/connectors/mysql.html#_spatial_data_types
        mappingMap.put("GEOMETRY",DataTypeMapping.STRUCT);
        mappingMap.put("LINESTRING",DataTypeMapping.STRUCT);
        mappingMap.put("POINT",DataTypeMapping.STRUCT);
        mappingMap.put("POLYGON",DataTypeMapping.STRUCT);
        mappingMap.put("MULTIPOINT",DataTypeMapping.STRUCT);
        mappingMap.put("MULTILINESTRING",DataTypeMapping.STRUCT);
        mappingMap.put("MULTIPOLYGON",DataTypeMapping.STRUCT);
        mappingMap.put("GEOMETRYCOLLECTION",DataTypeMapping.STRUCT);
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
//            if (relation == null)
//                throw new TCMException("not found DataType relation in "+column.getColumnInfo());
            column.setDataTypeMapping(relation);
        }
        table.setColumns(columns);
        return table;
    }


    /**
     * return table provides the mapping table to the PgSQL data type, according "*.core.DataTypeMapping"
     * @Param null :    table={...,PGSQL,..,columns={...,...,col_int,DataType=serial,dataTypeMapping=INT32}}
     * @Return: null :  table={null,MYSQL,..,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     */
    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        List<Column> sourceCols = cloneTable.getColumns();
        List<Column> cloneCols = new LinkedList<>();
        for (Column col : sourceCols){
            Column c = col.clone();
            c.setDataType(col.getDataTypeMapping().getOutDataType(DataSourceType.MYSQL));
            cloneCols.add(c);
        }
        cloneTable.setCatalogname(null);
        cloneTable.setSchemaname(null);
        cloneTable.setColumns(cloneCols);
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
            stringBuilder.append(column.getColumnName()).append(" ").append(column.getDataType());
            if (column.getDataType().equals("TIMESTAMP(3)"))
                stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)");
            if (Boolean.FALSE.equals(column.isNullAble()))
                stringBuilder.append(" not NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(")Engine InnoDB;");
        return stringBuilder.toString();
    }
}
