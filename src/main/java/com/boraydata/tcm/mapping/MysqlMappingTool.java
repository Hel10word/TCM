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

    static Map<String,DataTypeMapping> MappingMap = new HashMap<>();
    static {
        MappingMap.put("BOOLEAN",DataTypeMapping.BOOLEAN);
        MappingMap.put("BOOL",DataTypeMapping.BOOLEAN);
        MappingMap.put("BIT(1)",DataTypeMapping.BOOLEAN);
        MappingMap.put("BIT(>1)",DataTypeMapping.BYTES);
        MappingMap.put("BIT",DataTypeMapping.BYTES);
        MappingMap.put("TINYINT",DataTypeMapping.INT16);
        MappingMap.put("SMALLINT",DataTypeMapping.INT16);
        MappingMap.put("MEDIUMINT",DataTypeMapping.INT32);
        MappingMap.put("INT",DataTypeMapping.INT32);
        MappingMap.put("INTEGER",DataTypeMapping.INT32);
        MappingMap.put("BIGINT",DataTypeMapping.INT64);
        MappingMap.put("REAL",DataTypeMapping.FLOAT64);
        MappingMap.put("FLOAT",DataTypeMapping.FLOAT64);
        MappingMap.put("DOUBLE",DataTypeMapping.FLOAT64);
        MappingMap.put("CHAR",DataTypeMapping.STRING);
        MappingMap.put("VARCHAR",DataTypeMapping.STRING);
        MappingMap.put("BINARY",DataTypeMapping.BYTES);
        MappingMap.put("VARBINARY",DataTypeMapping.BYTES);
        MappingMap.put("TINYBLOB",DataTypeMapping.BYTES);
        MappingMap.put("TINYTEXT",DataTypeMapping.STRING);
        MappingMap.put("BLOB",DataTypeMapping.BYTES);
        MappingMap.put("TEXT",DataTypeMapping.STRING);
        MappingMap.put("MEDIUMBLOB",DataTypeMapping.BYTES);
        MappingMap.put("MEDIUMTEXT",DataTypeMapping.STRING);
        MappingMap.put("LONGBLOB",DataTypeMapping.BYTES);
        MappingMap.put("LONGTEXT",DataTypeMapping.STRING);
        MappingMap.put("JSON",DataTypeMapping.STRING);
        MappingMap.put("ENUM",DataTypeMapping.STRING);
        MappingMap.put("SET",DataTypeMapping.STRING);
        // YEAR[(2|4)]
        MappingMap.put("YEAR",DataTypeMapping.INT32);
        MappingMap.put("TIMESTAMP",DataTypeMapping.STRING);

        MappingMap.put("DATE",DataTypeMapping.INT32);
        MappingMap.put("TIME",DataTypeMapping.INT64);
        MappingMap.put("DATETIME",DataTypeMapping.INT64);

        // https://debezium.io/documentation/reference/1.0/connectors/mysql.html#_decimal_values
        MappingMap.put("NUMERIC",DataTypeMapping.BYTES);
        MappingMap.put("DECIMAL",DataTypeMapping.FLOAT64);

        // https://debezium.io/documentation/reference/1.0/connectors/mysql.html#_spatial_data_types
        MappingMap.put("GEOMETRY",DataTypeMapping.STRUCT);
        MappingMap.put("LINESTRING",DataTypeMapping.STRUCT);
        MappingMap.put("POINT",DataTypeMapping.STRUCT);
        MappingMap.put("POLYGON",DataTypeMapping.STRUCT);
        MappingMap.put("MULTIPOINT",DataTypeMapping.STRUCT);
        MappingMap.put("MULTILINESTRING",DataTypeMapping.STRUCT);
        MappingMap.put("MULTIPOLYGON",DataTypeMapping.STRUCT);
        MappingMap.put("GEOMETRYCOLLECTION",DataTypeMapping.STRUCT);
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
            c.setDataType(col.getDataTypeMapping().getOutDataType(DataSourceType.MYSQL));
            cloneCols.add(c);
        }
        cloneTable.setCatalogname(null);
        cloneTable.setSchemaname(null);
        cloneTable.setColumns(cloneCols);
        cloneTable.setDataSourceType(DataSourceType.MYSQL);
        return cloneTable;
    }

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
            if (Boolean.FALSE.equals(column.isNullAble()))
                stringBuilder.append("not NULL");
            stringBuilder.append("\n,");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(")Engine InnoDB;");
        return stringBuilder.toString();
    }
}
