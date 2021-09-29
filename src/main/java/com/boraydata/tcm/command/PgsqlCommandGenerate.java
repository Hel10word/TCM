package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;

/** Used to generate load and export shell statements in PgSQL
 * @author bufan
 * @data 2021/9/26
 */
public class PgsqlCommandGenerate implements CommandGenerate {
    // -- 导出
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"
    //--导入
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
    public String exportCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
        return exportCommand(config,filePath,tableName,delimiter,"");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, String tableName, String delimiter, String limit) {
        String sql = "select * from "+tableName;
        if (limit.length() >0 && limit != null)
            sql += " limit "+limit;
        return completeExportCommand(
                getConnectCommand(config),sql,filePath,delimiter
        );
    }

    @Override
    public String loadCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
        return completeLoadCommand(
                getConnectCommand(config),filePath,tableName,delimiter
        );
    }

}
