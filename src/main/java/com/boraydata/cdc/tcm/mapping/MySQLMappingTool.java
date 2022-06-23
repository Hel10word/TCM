package com.boraydata.cdc.tcm.mapping;


import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.StringUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * deal with the mapping relationship between MySQL Type and TCM Type
 * @since Fabric CDC V1.0 mainly uses the debezium plugin for parsing binlog log,so refer to part of the design.
 * design by :
 * @see <a href="https://debezium.io/documentation/reference/1.0/connectors/mysql.html#how-the-mysql-connector-maps-data-types_cdc"></a>
 * All Data Type from official document:
 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/data-types.html">MySQL 5.7 Data Type</a>
 * @author bufan
 * @date 2021/8/31
 */
public class MySQLMappingTool implements MappingTool {

    private static final Map<String, TCMDataTypeEnum> mappingMap = new LinkedHashMap<>();
    static {
        /**
         * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/numeric-types.html">numeric-types</a>
         */
        mappingMap.put("TINYINT", TCMDataTypeEnum.INT8);
        mappingMap.put("SMALLINT", TCMDataTypeEnum.INT16);
        mappingMap.put("MEDIUMINT", TCMDataTypeEnum.INT32);
        mappingMap.put("INT", TCMDataTypeEnum.INT32);
        mappingMap.put("BIGINT", TCMDataTypeEnum.INT64);
        mappingMap.put("DECIMAL", TCMDataTypeEnum.DECIMAL);
        mappingMap.put("FLOAT", TCMDataTypeEnum.FLOAT32);
        mappingMap.put("DOUBLE", TCMDataTypeEnum.FLOAT64);
        mappingMap.put("BIT", TCMDataTypeEnum.BYTES);
        mappingMap.put("TINYINT(1)", TCMDataTypeEnum.BOOLEAN);


//        mappingMap.put("INTEGER", TCMDataTypeEnum.INT32);
//        mappingMap.put("NUMERIC", TCMDataTypeEnum.DECIMAL);
//        mappingMap.put("REAL", TCMDataTypeEnum.FLOAT64);
//        mappingMap.put("DEC", TCMDataTypeEnum.DECIMAL);
//        mappingMap.put("FIXED", TCMDataTypeEnum.DECIMAL);
//        mappingMap.put("BOOL", TCMDataTypeEnum.BOOLEAN);
//        mappingMap.put("BOOLEAN", TCMDataTypeEnum.BOOLEAN);

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html">date-and-time-types</a>
         */
        mappingMap.put("DATE", TCMDataTypeEnum.DATE);
        mappingMap.put("TIME", TCMDataTypeEnum.TIME);
        mappingMap.put("DATETIME", TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("TIMESTAMP", TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("YEAR", TCMDataTypeEnum.INT32);

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/string-types.html">string-types</a>
         */
        mappingMap.put("CHAR", TCMDataTypeEnum.STRING);
        mappingMap.put("VARCHAR", TCMDataTypeEnum.STRING);
        mappingMap.put("BINARY", TCMDataTypeEnum.BYTES);
        mappingMap.put("VARBINARY", TCMDataTypeEnum.BYTES);
        mappingMap.put("BLOB", TCMDataTypeEnum.BYTES);
        mappingMap.put("TEXT", TCMDataTypeEnum.TEXT);
        mappingMap.put("TINYBLOB", TCMDataTypeEnum.BYTES);
        mappingMap.put("TINYTEXT", TCMDataTypeEnum.TEXT);
        mappingMap.put("MEDIUMBLOB", TCMDataTypeEnum.BYTES);
        mappingMap.put("MEDIUMTEXT", TCMDataTypeEnum.TEXT);
        mappingMap.put("LONGBLOB", TCMDataTypeEnum.BYTES);
        mappingMap.put("LONGTEXT", TCMDataTypeEnum.TEXT);
        mappingMap.put("ENUM", TCMDataTypeEnum.TEXT);
        mappingMap.put("SET", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/spatial-types.html">spatial-types</a>
         */
        mappingMap.put("GEOMETRY", TCMDataTypeEnum.TEXT);
        mappingMap.put("POINT", TCMDataTypeEnum.TEXT);
        mappingMap.put("LINESTRING", TCMDataTypeEnum.TEXT);
        mappingMap.put("POLYGON", TCMDataTypeEnum.TEXT);
        mappingMap.put("MULTIPOINT", TCMDataTypeEnum.TEXT);
        mappingMap.put("MULTILINESTRING", TCMDataTypeEnum.TEXT);
        mappingMap.put("MULTIPOLYGON", TCMDataTypeEnum.TEXT);
        mappingMap.put("GEOMETRYCOLLECTION", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/json.html">json</a>
         */
        mappingMap.put("JSON", TCMDataTypeEnum.TEXT);
    }

    @Override
    public Table createSourceMappingTable(Table table) {
        if(table.getColumns() == null || table.getColumns().isEmpty())
            throw new TCMException("Table.getColumns() is null or empty \n"+table.outTableInfo());
        List<Column> columns = table.getColumns();
        for (Column column : columns){
            if (StringUtil.isNullOrEmpty(column.getDataType()))
                throw new TCMException("not found DataType value in "+column);
            TCMDataTypeEnum relation = StringUtil.findRelation(mappingMap,column.getDataType(),null);
            if (Objects.isNull(relation))
                throw new TCMException("not found DataType relation in "+column);
            String colDataType = StringUtil.dataTypeFormat(column.getDataType());
//            if("TINYINT".equalsIgnoreCase(colDataType) && column.getCharacterMaximumPosition() == 1)
//                relation = TCMDataTypeEnum.BOOLEAN;
            if("FLOAT".equalsIgnoreCase(colDataType) && column.getNumericScale() != null)
                relation = TCMDataTypeEnum.FLOAT64;
            column.setTCMDataTypeEnum(relation);
        }
        table.setColumns(columns);
        if(table.getDataSourceEnum() == null)
            table.setDataSourceEnum(DataSourceEnum.MYSQL);
        if(table.getOriginalDataSourceEnum() == null)
            table.setOriginalDataSourceEnum(DataSourceEnum.MYSQL);
        return table;
    }



    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        if(table.getDataSourceEnum() != null && DataSourceEnum.MYSQL.equals(table.getDataSourceEnum()))
            return cloneTable;
        for (Column col : cloneTable.getColumns())
            col.setDataType(col.getTCMDataTypeEnum().getMappingDataType(DataSourceEnum.MYSQL));
        cloneTable.setDataSourceEnum(DataSourceEnum.MYSQL);
        return cloneTable;
    }

    /**
     *
     * generate the Create Table Statement by table
     * Refer to the official documentation (MySQL 5.7)
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/create-table.html">create-table</a>
     * e.g.
     *   Create Table If Not Exists `table name`(
     *      `column name` integer not null,
     *      `column name` varchar(20) not null,
     *      ....
     *   )ENGINE = InnoDB;
     *
     *  "ENGINE = InnoDB" is added by default, you can also not add;
     *
     * @Param Table : table={null,MySQL,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String
     */
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTableName() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceEnum().name());
        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists `"+table.getTableName()+"`(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            TCMDataTypeEnum tcmDataTypeEnum = column.getTCMDataTypeEnum();
            String dataType = column.getDataType();
            if(StringUtil.isNullOrEmpty(dataType) && Objects.isNull(tcmDataTypeEnum))
                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column);
            if(StringUtil.isNullOrEmpty(dataType))
                dataType = tcmDataTypeEnum.getMappingDataType(DataSourceEnum.MYSQL);
            stringBuilder.append("`").append(column.getColumnName()).append("` ").append(dataType);

            if(table.getOriginalDataSourceEnum() != null && DataSourceEnum.MYSQL.equals(table.getOriginalDataSourceEnum())){
                if(TCMDataTypeEnum.TIMESTAMP.equals(tcmDataTypeEnum)) {
                    if(column.getDatetimePrecision() > 0)
                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP(").append(column.getDatetimePrecision()).append(") ON UPDATE CURRENT_TIMESTAMP(").append(column.getDatetimePrecision()).append(")");
                    else
                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
                }if (Boolean.FALSE.equals(column.getNullable()))
                    stringBuilder.append(" not NULL");
                stringBuilder.append("\n,");
                continue;
            }

//            if (TCMDataTypeEnum.INT8.equals(tcmDataTypeEnum)){
//                Integer precision = column.getNumericPrecision();
//                if(precision != null){
//                    if(precision >= 1 && precision <= 255)
//                        stringBuilder.append("(").append(precision).append(")");
//                    else
//                        stringBuilder.append("(255)");
//                }else {
//                    // Nothing to do
//                    // MySQL Default is TINYINT(4)
//                }
//            } else if (TCMDataTypeEnum.INT16.equals(tcmDataTypeEnum)){
//                Integer precision = column.getNumericPrecision();
//                if(precision != null){
//                    if(precision >= 0 && precision <= 65535)
//                        stringBuilder.append("(").append(precision).append(")");
//                    else
//                        stringBuilder.append("(65535)");
//                }else {
//                    // Nothing to do
//                }
//            } else
             if(
                    TCMDataTypeEnum.FLOAT64.equals(tcmDataTypeEnum) ||
//                    TCMDataTypeEnum.FLOAT32.equals(tcmDataTypeEnum) ||
                    TCMDataTypeEnum.DECIMAL.equals(tcmDataTypeEnum)
            ){
                Integer precision = column.getNumericPrecision();
                Integer scale = column.getNumericScale();
                if(precision != null && precision != 0){
                    if(precision >= 1 && precision <= 65)
                        stringBuilder.append("(").append(precision);
                    else
                        stringBuilder.append("(65");
                    if(scale != null && scale != 0 ){
                        if(scale >= 1 && scale <= 30)
                            stringBuilder.append(",").append(scale);
                        else
                            stringBuilder.append(",30");
                    }
                    stringBuilder.append(")");
                }else {
                    // nothing to do
                    // MySQL Default is decimal(10,0)
                }
            }else if (
                    TCMDataTypeEnum.STRING.equals(tcmDataTypeEnum) ||
                    TCMDataTypeEnum.BYTES.equals(tcmDataTypeEnum)
            ){
                /**
                 * the VARCHAR or VARBINARY sometimes it cannot be set to (65535) , the field size is limited and many aspects.
                 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html"></a>
                 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html#innodb-row-format-compact"></a>
                 */
                Long characterMaximumPosition = column.getCharacterMaximumPosition();
                if(characterMaximumPosition != null){
                    if(characterMaximumPosition >= 0 && characterMaximumPosition <= 8192)
                        stringBuilder.append("(").append(characterMaximumPosition).append(")");
                    else
                        stringBuilder.append("(8192)");
                }else {
                    // Nothing to do
                }
            } else if(TCMDataTypeEnum.TIME.equals(tcmDataTypeEnum)){
                Integer datetimePrecision = column.getDatetimePrecision();
                if(datetimePrecision != null && datetimePrecision > 0 && datetimePrecision <= 6) {
                    stringBuilder.append("(").append(datetimePrecision).append(")");
                }else {
                    // Nothing to do
                }
            } else if (TCMDataTypeEnum.TIMESTAMP.equals(tcmDataTypeEnum)){
                Integer datetimePrecision = column.getDatetimePrecision();
                if(datetimePrecision != null){
                    if(datetimePrecision > 0 && datetimePrecision <= 6){
                        String fsp = "("+datetimePrecision+")";
                        stringBuilder.append(fsp)
                                .append(" DEFAULT CURRENT_TIMESTAMP").append(fsp)
                                .append(" ON UPDATE CURRENT_TIMESTAMP").append(fsp);
                    }
                    if(datetimePrecision == 0)
                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
                } else {
                    // nothing to do
                }
            }

            if (Boolean.FALSE.equals(column.getNullable()))
                stringBuilder.append(" not NULL");
            stringBuilder.append("\n,");
        }
        List<String> primaryKeys = table.getPrimaryKeys();
        if(Objects.nonNull(primaryKeys) && Boolean.FALSE.equals(primaryKeys.isEmpty()))
            stringBuilder.append("PRIMARY KEY(").append(String.join(",", primaryKeys)).append(")\n,");

        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(")ENGINE = InnoDB;");
        return stringBuilder.toString();
    }
}
