package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataTypeMapping;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;

import java.util.List;

/** Used to generate load and export shell statements in PgSQL
 * @author bufan
 * @data 2021/9/26
 */
public class PgsqlCommandGenerate implements CommandGenerate {
    // -- export
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"
    //-- load
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"

    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s/%s -c  \"?\" 2>&1",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getDatabasename());
    }
    private String completeExportCommand(String com,String sql,String filePath, String delimiter){
        return com.replace("?",
                "\\copy ("+sql+") to " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "\\copy "+tableName+" from " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter) {
        return exportCommand(config,filePath,table,delimiter,"");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter, String limit) {
        List<Column> columns = table.getColumns();
        StringBuffer sql = new StringBuffer("select ");
        for(Column c : columns){
            String columnName = c.getColumnName();
            if(c.getDataTypeMapping() == DataTypeMapping.BOOLEAN){
                // coalesce((pgboolean::boolean)::int,0) as pgboolean
                sql.append("coalesce(("+columnName+"::boolean)::int,0) as "+columnName);
            }else if (c.getDataType().equals("money")){
                // pgmoney::money::numeric as pgmoney3
                sql.append(columnName+"::money::numeric as "+columnName);
            }else {
                sql.append(columnName);
            }
            sql.append(",");
        }
        sql.deleteCharAt(sql.lastIndexOf(","));
        sql.append(" from ");
        sql.append(table.getTablename());
        if (limit.length() >0 && limit != null)
            sql.append(" limit ").append(limit);
        return completeExportCommand(
                getConnectCommand(config),sql.toString(),filePath,delimiter
        );
    }

    @Override
    public String loadCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
        return completeLoadCommand(
                getConnectCommand(config),filePath,tableName,delimiter
        );
    }

}
