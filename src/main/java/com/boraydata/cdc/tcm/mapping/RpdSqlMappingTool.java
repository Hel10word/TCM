package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.StringUtil;

import java.util.List;
import java.util.Objects;

/**
 * @author : bufan
 * @date : 2022/7/9
 *
 * @see <a href="https://docs.singlestore.com/db/v7.3/en/reference/sql-reference/data-types.html"></a>
 */
public class RpdSqlMappingTool extends MySQLMappingTool {

    /**
     *
     * generate the Create Table Statement by table
     * Refer to the official documentation (SingleStore v7.3)
     * @see <a href="https://docs.singlestore.com/db/v7.3/en/reference/sql-reference/data-definition-language-ddl/create-table.html?action=version-change">create-table</a>
     * e.g.
     *   Create rowstore Table If Not Exists `table name`(
     *      `column name` integer not null,
     *      `column name` varchar(20) not null,
     *      ....
     *   );
     *
     *   default use rowstore in RpdSQL
     *
     * @Param Table : table={null,MySQL,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String
     */
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTableName() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceEnum().name());
        StringBuilder stringBuilder = new StringBuilder("Create rowstore Table If Not Exists `"+table.getTableName()+"`(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            TCMDataTypeEnum tcmDataTypeEnum = column.getTcmDataTypeEnum();
            String dataType = column.getDataType();
            if(StringUtil.isNullOrEmpty(dataType) && Objects.isNull(tcmDataTypeEnum))
                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column);
            if(StringUtil.isNullOrEmpty(dataType))
                dataType = tcmDataTypeEnum.getMappingDataType(DataSourceEnum.RPDSQL);
            stringBuilder.append("`").append(column.getColumnName()).append("` ").append(dataType);

            if(table.getOriginalDataSourceEnum() != null && (DataSourceEnum.MYSQL.equals(table.getOriginalDataSourceEnum())) || DataSourceEnum.RPDSQL.equals(table.getOriginalDataSourceEnum())){
//                if(TCMDataTypeEnum.TIMESTAMP.equals(tcmDataTypeEnum)) {
//                    if(column.getDatetimePrecision() > 0)
//                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP(").append(column.getDatetimePrecision()).append(") ON UPDATE CURRENT_TIMESTAMP(").append(column.getDatetimePrecision()).append(")");
//                    else
//                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
//                }
                if (Boolean.FALSE.equals(column.getNullable()))
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
                    if(datetimePrecision == 0 || datetimePrecision == 6)
                        stringBuilder.append("(").append(datetimePrecision).append(")");
//                    if(datetimePrecision > 0 && datetimePrecision <= 6){
//                        String fsp = "("+datetimePrecision+")";
//                        stringBuilder.append(fsp)
//                                .append(" DEFAULT CURRENT_TIMESTAMP").append(fsp)
//                                .append(" ON UPDATE CURRENT_TIMESTAMP").append(fsp);
//                    }
//                    if(datetimePrecision == 0)
//                        stringBuilder.append(" DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
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
        stringBuilder.append(");");
        return stringBuilder.toString();
    }

}
