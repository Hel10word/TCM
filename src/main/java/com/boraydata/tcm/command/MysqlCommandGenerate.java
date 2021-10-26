package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.entity.Table;


/** Used to generate load and export shell statements in MySQL
 * @author bufan
 * @data 2021/9/24
 */
public class MysqlCommandGenerate implements CommandGenerate{

    // 1.0 this way to slow
    //-- export
    //mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem_1 limit 10"  | sed 's/"/""/g;s/\t/,/g;s/\n//g' > /usr/local/lineitem_1_limit_10.csv
    //-- load
    //mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
    // -sN  s: Produce less output  N: Not write column names in results
    // https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_skip-column-names



    // 2.0 use MySQL-Shell to export and load
    //-- export
    //mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem_mysql','/usr/local/test.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
    //-- load
    //mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.importTable('/usr/local/test.csv', {table: 'lineitem_mysql',linesTerminatedBy:'\n',fieldsTerminatedBy:',',bytesPerChunk:'500M'})"

    private String getConnectCommand(DatabaseConfig config){

        return String.format("mysqlsh -h%s -P%s -u%s -p%s --database %s -e \"?\" 2>&1",
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabasename());
    }
    private String completeExportCommand(String com,String filePath, String tableName,String delimiter){
        return com.replace("?",
                "util.exportTable('"+tableName+"','"+filePath+"',{linesTerminatedBy:'\\n',fieldsTerminatedBy:'"+delimiter+"'})");
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "util.importTable('"+filePath+"', {table: '"+tableName+"',fieldsTerminatedBy:'"+delimiter+"',bytesPerChunk:'100M',maxRate:'100M',threads:'10'})");
//                "util.importTable('"+filePath+"', {table: '"+tableName+"',linesTerminatedBy:'\\n',fieldsTerminatedBy:'"+delimiter+"',bytesPerChunk:'100M',maxRate:'100M',threads:'10'})");
    }
    @Override
    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter) {
        return exportCommand(config,filePath,table,delimiter,"");
    }

    @Override
    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter, String limit) {
        return completeExportCommand(getConnectCommand(config),filePath,table.getTablename(),delimiter);
    }

    @Override
    public String loadCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
        return LoadByMysqlCli(config,filePath,tableName,delimiter);
//        return completeLoadCommand(getConnectCommand(config),filePath,tableName,delimiter);
    }

    public String LoadByMysqlCli(DatabaseConfig config,String filePath, String tableName, String delimiter){
//mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"

        return String.format("mysql -h "+config.getHost()+" -P "+config.getPort()+" -u"+config.getUsername()+" -p"+config.getPassword()+" --database "+config.getDatabasename()+" -e \"load data local infile '%s' into table %s fields terminated by '%s' ;\""
                ,filePath
                ,tableName
                ,delimiter);

    }


}
