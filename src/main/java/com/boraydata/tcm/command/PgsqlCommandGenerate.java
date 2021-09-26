package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;

/** Used to generate load and export shell statements in PgSQL
 * @author bufan
 * @data 2021/9/26
 */
public class PgsqlCommandGenerate implements CommandGenerate {
    // -- 导出
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with csv;"
    //--导入
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with csv;"

    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s/%s -c  \"?\"",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getDatabasename());
    }
    private String completeExportCommand(String com,String sql,String filePath){
        return com.replace("?",
                "\\copy ("+sql+") to " +
                        "'"+filePath+"' with csv;");
    }
    private String completeLoadCommand(String com,String filePath, String tableName){
        return com.replace("?",
                "\\copy "+tableName+" from " +
                        "'"+filePath+"' with csv;");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, String tableName) {
        return exportCommand(config,filePath,tableName,"select * from "+tableName);
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, String tableName, String sql) {
        return completeExportCommand(
                getConnectCommand(config),sql,filePath
        );
    }

    @Override
    public String loadCommand(DatabaseConfig config, String filePath, String tableName) {
        return completeLoadCommand(
                getConnectCommand(config),filePath,tableName
        );
    }

}
