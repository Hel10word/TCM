package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;


/** Used to generate load and export shell statements in MySQL
 * @author bufan
 * @data 2021/9/24
 */
public class MysqlCommandGenerate implements CommandGenerate{

    //--导出
    //mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem_1 limit 10"  | sed 's/"/""/g;s/\t/,/g;s/\n//g' > /usr/local/lineitem_1_limit_10.csv
    //--导入
    //mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"

    private String getConnectCommand(DatabaseConfig config){
        // -sN  s: Produce less output  N: Not write column names in results
        // https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_skip-column-names
        return String.format("mysql -h %s -P %s -u%s -p%s --database %s -sN -e ? 2>&1",
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabasename());
    }
    private String completeExportCommand(String com,String sql,String filePath, String delimiter){
        return com.replace("?",
                "\""+sql+"\""+
                        String.format("| sed 's/\"/\"\"/g;s/\\t/"+delimiter+"/g;s/\\n//g' >> %s ",filePath));
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "\""+
                        String.format("load data local infile '%s' into table %s fields terminated by '"+delimiter+"' lines terminated by '\\n';"
                                ,filePath,tableName)+
                        "\""
                );
    }
    @Override
    public String exportCommand(DatabaseConfig config, String filePath, String tableName,String delimiter) {
        return exportCommand(config,filePath,tableName,delimiter,"");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, String tableName, String delimiter, String limit) {
        String sql = "select * from "+tableName;
        if (limit.length() >0 && limit != null){
            sql += " limit "+limit;
            return completeExportCommand(getConnectCommand(config),sql,filePath,delimiter);
        }

        String shell = "";
        int limitNum = 3000_0000;
        for (int i = 0;i<3;i++)
            shell += completeExportCommand(getConnectCommand(config),sql+" limit "+(limitNum*i)+","+limitNum,filePath,delimiter)+"\n";
        return shell;
    }

    @Override
    public String loadCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
        return completeLoadCommand(
                getConnectCommand(config),filePath,tableName,delimiter
        );
    }
}
