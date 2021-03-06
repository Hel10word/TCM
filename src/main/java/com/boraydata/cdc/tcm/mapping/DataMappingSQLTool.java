package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.boraydata.cdc.tcm.entity.Table;

import java.util.Properties;

/**
 * create mapping table data query statement
 * Since the table data is exported as a CSV file from the Source and then imported to the Sink,
 * some special field types need to be processed in this process.
 *
 *  E.g. PgSQL{boolean:t,boolean:f}  =>  MySQL{boolean:0,boolean:1}  =>  Spark{boolean:false,boolean:true}
 *
 *  MySQL、RpdSQL Boolean export 0;1;''  load support 0;1;''
 *  PostgreSQL   Boolean export f;t     load support true,yes,on,1,t;false,no,off,0;''
 *  SQL Server  Boolean export 0;1;''   load support 0;1;''
 *  HUDI        Boolean                 load support false;true;''
 *
 *            |   Source  |   Clone   | Boolean | Byte | Money |
 *            | --------- | --------- | ------- | ---- | ----- |
 *            |   MySQL   |    MySQL  |         |  √√√ |       |
 *            |   RpdSQL  |    PgSQL  |         |  √√√ |       |
 *            |           | SqlServer |         |  √√√ |       |
 *            |           |    HUDI   |   √√√   |  √√√ |       |
 *            | --------- | --------- | ------- | ---- | ----- |
 *            |   PgSQL   |    MySQL  |   √√√   |      |  √√√  |
 *            |           |    PgSQL  |         |      |       |
 *            |           | SqlServer |   √√√   |      |       |
 *            |           |    HUDI   |   √√√   |      |  √√√  |
 *            | --------- | --------- | ------- | ---- | ----- |
 *            | SqlServer |    MySQL  |         |      |       |
 *            |           |    PgSQL  |         |      |       |
 *            |           | SqlServer |         |      |       |
 *            |           |    HUDI   |   √√√   |      |       |
 *
 *      √√√ : need to create Temp table or use mapping query statement.
 * @author bufan
 * @date 2021/11/5
 */
public class DataMappingSQLTool {
    private static final String TRUE = "_TRUE";
    private static final String FALSE = "_FALSE";
    private static final Properties mappingPts;
    static {
        mappingPts = new Properties();
        mappingPts.put(DataSourceEnum.MYSQL+TRUE,"'1'");
        mappingPts.put(DataSourceEnum.MYSQL+FALSE,"'0'");
        mappingPts.put(DataSourceEnum.POSTGRESQL +TRUE,"'t'");
        mappingPts.put(DataSourceEnum.POSTGRESQL +FALSE,"'f'");
        mappingPts.put(DataSourceEnum.SQLSERVER +TRUE,"'1'");
        mappingPts.put(DataSourceEnum.SQLSERVER +FALSE,"'0'");
        mappingPts.put(DataSourceEnum.RPDSQL+TRUE,"'1'");
        mappingPts.put(DataSourceEnum.RPDSQL+FALSE,"'0'");
        mappingPts.put(DataSourceEnum.HUDI+TRUE,"'true'");
        mappingPts.put(DataSourceEnum.HUDI+FALSE,"'false'");
    }

    /**
     * return an Mapping Query SQL like :
     * @Return: select CASE col_boolean WHEN '1' THEN 'true' WHEN '0' THEN 'false' ELSE 'false' END as col_boolean from testTable;
     */
    public static String getMappingDataSQL(Table table, DataSourceEnum target){
        StringBuilder sbSQL = new StringBuilder();
        DataSourceEnum tableType = table.getOriginalDataSourceEnum();
        sbSQL.append("select ");
        for (Column column : table.getColumns()){
            TCMDataTypeEnum colType = column.getTcmDataTypeEnum();
            String colName = column.getColumnName();
            if(tableType.equals(target)){
                sbSQL.append(colName);
            }else if(DataSourceEnum.POSTGRESQL.equals(tableType) && "MONEY".equalsIgnoreCase(StringUtil.dataTypeFormat(column.getDataType()))){
//                pgmoney::money::numeric as pgmoney3
                sbSQL.append("(")
                        .append(colName)
                        .append("::money::numeric) as ")
                        .append(colName);
            }else if(TCMDataTypeEnum.BYTES.equals(colType) && (DataSourceEnum.MYSQL.equals(tableType) || DataSourceEnum.RPDSQL.equals(tableType))){
                String dataType = column.getDataType().replaceAll("\\(.*\\)","");
                if(
                        dataType.equalsIgnoreCase("BINARY") ||
                        dataType.equalsIgnoreCase("VARBINARY") ||
                        dataType.equalsIgnoreCase("BLOB") ||
                        dataType.equalsIgnoreCase("MEDIUMBLOB") ||
                        dataType.equalsIgnoreCase("LONGBLOB")
                ){
//          select mybit1,mybit5,HEX(mybinary),HEX(myvarbinary),HEX(myblob),HEX(mymediumblob),HEX(mylongblob) from byte_types_mysql;
                    sbSQL.append("CONCAT('0x',HEX(")
                            .append(colName)
                            .append(")) as ")
                            .append(colName);
                }else if(dataType.equalsIgnoreCase("BIT")){
                    sbSQL.append("BIN(")
                            .append(colName)
                            .append(") as ")
                            .append(colName);
                }else
                    sbSQL.append(colName);
            }else if(TCMDataTypeEnum.BOOLEAN.equals(colType)){
                /**
                 *  CASE v
                 *       WHEN 2 THEN SELECT v;
                 *       WHEN 3 THEN SELECT 0;
                 *       ELSE
                 *         BEGIN
                 *         END;
                 *     END CASE;
                 */
                sbSQL.append("CASE ").append(colName)
                        .append(" WHEN ").append(mappingPts.get(tableType+TRUE)).append(" THEN ").append(mappingPts.get(target+TRUE))
                        .append(" WHEN ").append(mappingPts.get(tableType+FALSE)).append(" THEN ").append(mappingPts.get(target+FALSE))
                        .append(" ELSE ").append(mappingPts.get(target+FALSE))
                        .append(" END as ").append(colName);
            }else {
                sbSQL.append(colName);
            }
            sbSQL.append(",");
        }
        sbSQL.deleteCharAt(sbSQL.length()-1);
        sbSQL.append(" from ").append(table.getTableName());
//                .append(";");
        return sbSQL.toString();
    }


    /**
     * According to the relationship and the sourceTable to check whether a tempTable is needed.
     *      ps: MySQL export SQL grammar not support export table data by query statement,So should create Temp Table.
     *          PostgreSQL export SQL grammar support query statement,not need to create Temp Table,
     */
    public static Table checkRelationship(Table table,DataSourceEnum sourceType,DataSourceEnum cloneType){
        boolean mappingFlag = Boolean.FALSE;
        for (Column col: table.getColumns()){
            if(mappingFlag)break;
            TCMDataTypeEnum colType = col.getTcmDataTypeEnum();
            if(sourceType.equals(DataSourceEnum.MYSQL) || sourceType.equals(DataSourceEnum.RPDSQL)){
                if(colType.equals(TCMDataTypeEnum.BOOLEAN) && cloneType.equals(DataSourceEnum.HUDI)){
                    mappingFlag = Boolean.TRUE;
                }else if (colType.equals(TCMDataTypeEnum.BYTES)){
//                    if(cloneType.equals(DataSourceEnum.MYSQL) || cloneType.equals(DataSourceEnum.POSTGRESQL) || cloneType.equals(DataSourceEnum.RPDSQL) || cloneType.equals(DataSourceEnum.HUDI))
                        mappingFlag = Boolean.TRUE;
                }
            }else if (sourceType.equals(DataSourceEnum.POSTGRESQL)){
                if(colType.equals(TCMDataTypeEnum.BOOLEAN) || "MONEY".equalsIgnoreCase(StringUtil.dataTypeFormat(col.getDataType()))){
                    if(!cloneType.equals(DataSourceEnum.POSTGRESQL))
                        mappingFlag = Boolean.TRUE;
                }
            }else if(sourceType.equals(DataSourceEnum.SQLSERVER)){
                if(colType.equals(TCMDataTypeEnum.BOOLEAN) && cloneType.equals(DataSourceEnum.HUDI))
                    mappingFlag = Boolean.TRUE;
            }
        }
        if(Boolean.TRUE.equals(mappingFlag)){
            Table clone = table.clone();
            for (Column col : clone.getColumns())
                col.setDataType(TCMDataTypeEnum.TEXT.getMappingDataType(table.getDataSourceEnum()));
            clone.setDataSourceEnum(table.getDataSourceEnum());
            clone.setTableName(table.getTableName()+"_"+ StringUtil.getRandom()+"_temp");
            return clone;
        }
        return null;
    }


    // generate TempTable by source Table.clone() , all colDataType become TEXT , Table Name is Random
//    private static Table createTempTable(Table table){
//        Table clone = table.clone();
//        for (Column col : clone.getColumns())
//            col.setDataType(TCMDataTypeEnum.TEXT.getOutDataType(table.getDataSourceEnum()));
//        clone.setDataSourceEnum(table.getDataSourceEnum());
//        clone.setTableName(table.getTableName()+"_"+ StringUtil.getRandom()+"_temp");
//        return clone;
//    }



}
