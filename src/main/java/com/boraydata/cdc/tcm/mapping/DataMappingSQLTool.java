package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.boraydata.cdc.tcm.core.TableCloneManageContext;
import com.boraydata.cdc.tcm.entity.Table;

import java.util.Properties;

/**
 * create mapping table data query statement
 * Since the table data is exported as a CSV file from the Source and then imported to the Sink,
 * some special field types need to be processed in this process.
 *
 *  E.g. PgSQL{boolean:t,boolean:f}  =>  MySQL{boolean:0,boolean:1}  =>  Spark{boolean:false,boolean:true}
 *
 *            |   Source  |   Clone   | Boolean | Byte | Money |
 *            | --------- | --------- | ------- | ---- | ----- |
 *            |   MySQL   |    MySQL  |         |  √√√ |       |
 *            |           |    PgSQL  |         |  √√√ |       |
 *            |           |    Hudi   |   √√√   |  √√√ |       |
 *            | --------- | --------- | ------- | ---- | ----- |
 *            |   PgSQL   |    MySQL  |   √√√   |      |  √√√  |
 *            |           |    PgSQL  |         |      |       |
 *            |           |    Hudi   |   √√√   |      |  √√√  |
 *
 *      √√√ : need to create Temp table or use mapping query statement.
 * @author bufan
 * @data 2021/11/5
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
            TCMDataTypeEnum colType = column.getTCMDataTypeEnum();
            String colName = column.getColumnName();
            if(tableType.equals(target)){
                sbSQL.append(colName);
            }else if(DataSourceEnum.POSTGRESQL.equals(tableType) && "MONEY".equalsIgnoreCase(StringUtil.dataTypeFormat(column.getDataType()))){
//                pgmoney::money::numeric as pgmoney3
                sbSQL.append("(")
                        .append(colName)
                        .append("::money::numeric) as ")
                        .append(colName);
            }else if(TCMDataTypeEnum.BYTES.equals(colType) && DataSourceEnum.MYSQL.equals(tableType)){
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
        Boolean mappingFlag = Boolean.FALSE;
        for (Column col: table.getColumns()){
            TCMDataTypeEnum colType = col.getTCMDataTypeEnum();
            if(sourceType.equals(DataSourceEnum.MYSQL)){
                if(colType.equals(TCMDataTypeEnum.BOOLEAN)){
                    if(cloneType.equals(DataSourceEnum.HUDI)){
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }else if (colType.equals(TCMDataTypeEnum.BYTES)){
                    if(cloneType.equals(DataSourceEnum.MYSQL) || cloneType.equals(DataSourceEnum.POSTGRESQL) || cloneType.equals(DataSourceEnum.HUDI)){
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }
            }else if (sourceType.equals(DataSourceEnum.POSTGRESQL)){
                if(colType.equals(TCMDataTypeEnum.BOOLEAN) || "MONEY".equalsIgnoreCase(StringUtil.dataTypeFormat(col.getDataType()))){
                    if(cloneType.equals(DataSourceEnum.MYSQL) || cloneType.equals(DataSourceEnum.HUDI)){
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }
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
