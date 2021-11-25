package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;

import java.util.Properties;

/** create Mapping query statement
 *  E.g. PgSQL{boolean:t,boolean:f}  =>  MySQL{boolean:0,boolean:1}  =>  Spark{boolean:false,boolean:true}
 * @author bufan
 * @data 2021/11/5
 */
public class DataMappingSQLTool {
    final static String TRUE = "_TRUE";
    final static String FALSE = "_FALSE";
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

    public static String getSQL(Table table,DataSourceType target){
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
        sbSQL.append(" from ").append(table.getTablename()).append(";");
        return sbSQL.toString();
    }
}
