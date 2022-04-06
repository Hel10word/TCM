package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.utils.StringUtil;

import java.util.Properties;

/** create Mapping query statement
 *  E.g. PgSQL{boolean:t,boolean:f}  =>  MySQL{boolean:0,boolean:1}  =>  Spark{boolean:false,boolean:true}
 * @author bufan
 * @data 2021/11/5
 */
public class DataMappingSQLTool {
    static String TRUE = "_TRUE";
    static String FALSE = "_FALSE";
    static Properties mappingPts;
    static {
        mappingPts = new Properties();
        mappingPts.put(DataSourceType.MYSQL+TRUE,"'1'");
        mappingPts.put(DataSourceType.MYSQL+FALSE,"'0'");
        mappingPts.put(DataSourceType.POSTGRES+TRUE,"'t'");
        mappingPts.put(DataSourceType.POSTGRES+FALSE,"'f'");
        mappingPts.put(DataSourceType.HUDI+TRUE,"'true'");
        mappingPts.put(DataSourceType.HUDI+FALSE,"'false'");
    }
    // return an Mapping Query SQL like :
    // select CASE col_boolean WHEN '1' THEN 'true' WHEN '0' THEN 'false' ELSE 'false' END as col_boolean from testTable;
    public static String getMappingDataSQL(Table table, DataSourceType target){
        StringBuilder sbSQL = new StringBuilder();
        DataSourceType tableType = table.getSourceType();
        sbSQL.append("select ");
        for (Column column : table.getColumns()){
            TableCloneManageType colType = column.getTableCloneManageType();
            String colName = column.getColumnName();
            if(tableType.equals(target)){
                sbSQL.append(colName);
            }else if(TableCloneManageType.MONEY.equals(colType) && DataSourceType.POSTGRES.equals(tableType)){
//                pgmoney::money::numeric as pgmoney3
                sbSQL.append("(")
                        .append(colName)
                        .append("::money::numeric) as ")
                        .append(colName);
            }else if(TableCloneManageType.BYTES.equals(colType) && DataSourceType.MYSQL.equals(tableType)){
                String dataType = column.getDataType().replaceAll("\\(.*\\)","");
                if(
                        dataType.equalsIgnoreCase("BINARY") ||
                        dataType.equalsIgnoreCase("VARBINARY") ||
                        dataType.equalsIgnoreCase("BLOB") ||
                        dataType.equalsIgnoreCase("MEDIUMBLOB") ||
                        dataType.equalsIgnoreCase("LONGBLOB")
                ){
//                    select mybit1,mybit5,HEX(mybinary),HEX(myvarbinary),HEX(myblob),HEX(mymediumblob),HEX(mylongblob) from byte_types_mysql;
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
            }else if(TableCloneManageType.BOOLEAN.equals(colType)){
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
     *      |   Source  |   Clone   | Boolean | Byte | Money |
     *      | --------- | --------- | ------- | ---- | ----- |
     *      |   MySQL   |    MySQL  |         |  √√√ |       |
     *      |           |    PgSQL  |         |  √√√ |       |
     *      |           |    Hudi   |   √√√   |  √√√ |       |
     *      | --------- | --------- | ------- | ---- | ----- |
     *      |   PgSQL   |    MySQL  |   √√√   |      |  √√√  |
     *      |           |    PgSQL  |         |      |       |
     *      |           |    Hudi   |   √√√   |      |  √√√  |
     *
     *      MySQL Temp need create TempTable，PgSQL just change select statement;
     */
    // According to the relationship and the sourceTable to check whether a tempTable is needed.
    public static void checkRelationship(Table table,MappingTool sourceTool, TableCloneManageContext tcmContext){
        DataSourceType sourceType = tcmContext.getSourceConfig().getDataSourceType();
        DataSourceType cloneType = tcmContext.getSourceConfig().getDataSourceType();
        Boolean mappingFlag = Boolean.FALSE;
        for (Column col: table.getColumns()){
            TableCloneManageType colType = col.getTableCloneManageType();
            if(sourceType.equals(DataSourceType.MYSQL)){
                if(colType.equals(TableCloneManageType.BOOLEAN)){
                    if(cloneType.equals(DataSourceType.HUDI)){
//                        tcmContext.setTempTable(createTempTable(table));
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }else if (colType.equals(TableCloneManageType.BYTES)){
                    if(cloneType.equals(DataSourceType.MYSQL) || cloneType.equals(DataSourceType.POSTGRES) || cloneType.equals(DataSourceType.HUDI)){
//                        tcmContext.setTempTable(createTempTable(table));
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }
            }else if (sourceType.equals(DataSourceType.POSTGRES)){
                if(colType.equals(TableCloneManageType.BOOLEAN) || colType.equals(TableCloneManageType.MONEY)){
                    if(cloneType.equals(DataSourceType.MYSQL) || cloneType.equals(DataSourceType.HUDI)){
//                        tcmContext.setTempTable(table.clone());
                        mappingFlag = Boolean.TRUE;
                        break;
                    }
                }
            }
        }
        if(Boolean.TRUE.equals(mappingFlag)){
            Table clone = table.clone();
            for (Column col : clone.getColumns())
                col.setDataType(TableCloneManageType.TEXT.getOutDataType(table.getDataSourceType()));
            clone.setDataSourceType(table.getDataSourceType());
            clone.setTableName(table.getTableName()+"_"+ StringUtil.getRandom()+"_temp");
            tcmContext.setTempTable(clone);
            tcmContext.setTempTableCreateSQL(sourceTool.getCreateTableSQL(clone));
            tcmContext.setTempTableSelectSQL(getMappingDataSQL(table,cloneType));
        }




    }

    // generate TempTable by source Table.clone() , all colDataType become TEXT , Table Name is Random
//    private static Table createTempTable(Table table){
//        Table clone = table.clone();
//        for (Column col : clone.getColumns())
//            col.setDataType(TableCloneManageType.TEXT.getOutDataType(table.getDataSourceType()));
//        clone.setDataSourceType(table.getDataSourceType());
//        clone.setTableName(table.getTableName()+"_"+ StringUtil.getRandom()+"_temp");
//        return clone;
//    }



}
