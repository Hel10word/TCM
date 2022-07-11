package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.StringUtil;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * deal with the mapping relationship between MySQL Type and TCM Type
 * @since Fabric CDC V1.0 mainly uses the debezium plugin for parsing binlog log,so refer to part of the design.
 * design by :
 * @see <a href="https://debezium.io/documentation/reference/1.0/connectors/sqlserver.html#sqlserver-data-types"></a>
 * All Data Type from official document:
 * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#data-type-categories">SQL Server 2019 Data Type</a>
 * @author : bufan
 * @date : 2022/6/8
 */

public class SqlServerMappingTool implements MappingTool{
    private static final Map<String, TCMDataTypeEnum> mappingMap = new LinkedHashMap<>();
    static {
        /**
         * Exact numerics
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#exact-numerics"></a>
         */
        mappingMap.put("bit",TCMDataTypeEnum.BOOLEAN);
        mappingMap.put("decimal",TCMDataTypeEnum.DECIMAL);
        mappingMap.put("numeric",TCMDataTypeEnum.DECIMAL);
        mappingMap.put("tinyint",TCMDataTypeEnum.INT8);
        mappingMap.put("smallint",TCMDataTypeEnum.INT16);
        mappingMap.put("int",TCMDataTypeEnum.INT32);
        mappingMap.put("bigint",TCMDataTypeEnum.INT64);
        mappingMap.put("smallmoney",TCMDataTypeEnum.DECIMAL);
        mappingMap.put("money",TCMDataTypeEnum.DECIMAL);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#approximate-numerics"></a>
         */
        mappingMap.put("real",TCMDataTypeEnum.FLOAT32);
        mappingMap.put("float",TCMDataTypeEnum.FLOAT64);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#date-and-time"></a>
         */
        mappingMap.put("date",TCMDataTypeEnum.DATE);
        mappingMap.put("time",TCMDataTypeEnum.TIME);
        mappingMap.put("datetime",TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("datetime2",TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("datetimeoffset",TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("smalldatetime",TCMDataTypeEnum.TIMESTAMP);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#character-strings"></a>
         */
        mappingMap.put("char",TCMDataTypeEnum.STRING);
        mappingMap.put("varchar",TCMDataTypeEnum.STRING);
        mappingMap.put("text",TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#unicode-character-strings"></a>
         */
        mappingMap.put("nchar",TCMDataTypeEnum.STRING);
        mappingMap.put("nvarchar",TCMDataTypeEnum.STRING);
        mappingMap.put("ntext",TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#binary-strings"></a>
         */
        mappingMap.put("binary",TCMDataTypeEnum.BYTES);
        mappingMap.put("varbinary",TCMDataTypeEnum.BYTES);

        /**
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#other-data-types"></a>
         */
        // todo SQL Server Other Type need Test


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
            String dataType = StringUtil.dataTypeFormat(column.getDataType());
            if("smallmoney".equalsIgnoreCase(dataType))
                column.setNumericPrecision(6).setNumericScale(4);
            if("money".equalsIgnoreCase(dataType))
                column.setNumericPrecision(15).setNumericScale(4);
            column.setTcmDataTypeEnum(relation);
        }
        table.setColumns(columns);
        return table;
    }



    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        if(table.getDataSourceEnum() != null && DataSourceEnum.SQLSERVER.equals(table.getDataSourceEnum()))
            return cloneTable;
        for (Column col : cloneTable.getColumns())
            col.setDataType(col.getTcmDataTypeEnum().getMappingDataType(DataSourceEnum.SQLSERVER));
        return cloneTable;
    }

    /**
     *
     * generate the Create Table Statement by table
     * Refer to the official documentation ( SQL Server 2019 )
     * @see <a href="https://docs.microsoft.com/zh-cn/sql/t-sql/statements/create-table-transact-sql?view=sql-server-linux-ver15">create-table</a>
     * e.g.
     *   Create Table table name(
     *      column name int not null,
     *      column name nvarchar(20) not null,
     *      ....
     *   );
     *
     * @Param Table : table={null,SQLSERVER,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String
     * @see <a href="https://database.guide/2-ways-to-create-a-table-if-it-doesnt-exist-in-sql-server/"></a>
     * @see <a href="https://stackoverflow.com/questions/6520999/create-table-if-not-exists-equivalent-in-sql-server"></a>
     */
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTableName() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceEnum().name());

        StringBuilder stringBuilder = new StringBuilder();
        String schemaName = table.getSchemaName();
        if(StringUtil.isNullOrEmpty(schemaName))
            schemaName = "dbo";
        String tableName = schemaName+"."+table.getTableName();
        //        IF OBJECT_ID(N'dbo.t1',N'U') IS NULL
        stringBuilder.append("IF OBJECT_ID(N'").append(tableName).append("',N'U') IS NULL \n");


        stringBuilder.append("Create Table "+tableName+"(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            TCMDataTypeEnum tcmDataTypeEnum = column.getTcmDataTypeEnum();
            String dataType = column.getDataType();
            if(StringUtil.isNullOrEmpty(dataType) && Objects.isNull(tcmDataTypeEnum))
                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column);
            if(StringUtil.isNullOrEmpty(dataType))
                dataType = tcmDataTypeEnum.getMappingDataType(DataSourceEnum.SQLSERVER);

            stringBuilder.append(column.getColumnName()).append(" ").append(dataType);

            if(
                    TCMDataTypeEnum.STRING.equals(tcmDataTypeEnum)
            ){
                Long characterMaximumPosition = column.getCharacterMaximumPosition();
                if(characterMaximumPosition != null){
                    if(characterMaximumPosition >= 0 && characterMaximumPosition <= 4000)
                        stringBuilder.append("(").append(characterMaximumPosition).append(")");
                    else
                        stringBuilder.append("(max)");
                }else {
                    // Nothing to do
                }
            } else if(
                    TCMDataTypeEnum.BYTES.equals(tcmDataTypeEnum)
            ){
                Long characterMaximumPosition = column.getCharacterMaximumPosition();
                if(characterMaximumPosition != null){
                    if(characterMaximumPosition >= 0 && characterMaximumPosition <= 8000)
                        stringBuilder.append("(").append(characterMaximumPosition).append(")");
                    else
                        stringBuilder.append("(max)");
                }else {
                    // Nothing to do
                }
            }else if(
                    TCMDataTypeEnum.TIME.equals(tcmDataTypeEnum) ||
                    TCMDataTypeEnum.TIMESTAMP.equals(tcmDataTypeEnum)
            ){
                Integer datetimePrecision = column.getDatetimePrecision();
                if(datetimePrecision != null && datetimePrecision != 0){
                    if(
                            datetimePrecision > 0 && datetimePrecision <= 7 &&
                            Boolean.FALSE.equals("datetime".equalsIgnoreCase(dataType))
                    ) {
                        stringBuilder.append("(").append(datetimePrecision).append(")");
                    }else{}
                }else {
                    // Nothing to do
                }
            } else if(
                    TCMDataTypeEnum.DECIMAL.equals(tcmDataTypeEnum) &&
                    Boolean.FALSE.equals("smallmoney".equalsIgnoreCase(dataType)) &&
                    Boolean.FALSE.equals("money".equalsIgnoreCase(dataType))
            ){
                Integer precision = column.getNumericPrecision();
                Integer scale = column.getNumericScale();
                if(precision != null && precision != 0){
                    if(precision >= 1 && precision <= 38) {
                        stringBuilder.append("(").append(precision);
                    }else {
                        precision = 38;
                        stringBuilder.append("(38");
                    }
                    if(scale != null && scale != 0 ){
                        if(scale >= 1 && scale <= precision)
                            stringBuilder.append(",").append(scale);
                        else
                            stringBuilder.append(",").append(precision);
                    }
                    stringBuilder.append(")");
                }else {
                    // nothing to do
                    // SQL Server default is decimal(18,0)
                    stringBuilder.append("(38,24)");
                }
            }else {
                // Nothing to do
            }
            if (Boolean.FALSE.equals(column.getNullable()))
                stringBuilder.append(" NOT NULL");
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
