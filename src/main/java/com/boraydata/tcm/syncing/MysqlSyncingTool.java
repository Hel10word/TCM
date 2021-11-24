package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;


/** Export and Load Table Data in MySQL,by shell statements.
 * @author bufan
 * @data 2021/9/24
 */
public class MysqlSyncingTool implements SyncingTool {
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


    @Override
    public boolean exportFile(DatabaseConfig config, AttachConfig attachConfig) {
        String exportCommand =  exportCommand(config,attachConfig);
        String exportShellPath = attachConfig.getExportShellPath();

//        System.out.println(exportCommand);
//        return true;


        if(FileUtil.WriteMsgToFile(exportCommand,exportShellPath)){
            if(FileUtil.Exists(exportShellPath))
                return CommandExecutor.execuetShell(exportShellPath,attachConfig.getDebug());
        }
        return false;
    }

    @Override
    public boolean loadFile(DatabaseConfig config, AttachConfig attachConfig) {
        String loadCommand = loadCommand(config, attachConfig);
        String loadShellPath = attachConfig.getLoadShellPath();

//        System.out.println(loadCommand);
//        return true;

        if(FileUtil.WriteMsgToFile(loadCommand,loadShellPath)){
            if(FileUtil.Exists(loadShellPath))
                return CommandExecutor.execuetShell(loadShellPath,attachConfig.getDebug());
        }
        return false;
    }

    /**
     *
     * @Param config :
     * @Param command : mysql
     * @Return: String : mysql -h 127.0.0.1 -P 3306 -uroot  -proot --datavase test_db -e "?"
     */
    private String getConnectCommand(DatabaseConfig config,String command){
        return String.format("%s -h %s -P %s -u%s -p%s --database %s -e \"?\" 2>&1",
                command,
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabasename());
    }
    private String completeExportCommand(String conCommand,String filePath, String tableName,String delimiter){
        return conCommand.replace("?",
                "util.exportTable('"+tableName+"','"+filePath+"',{linesTerminatedBy:'\\n',fieldsTerminatedBy:'"+delimiter+"'})");
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "util.importTable('"+filePath+"', {table: '"+tableName+"',fieldsTerminatedBy:'"+delimiter+"',bytesPerChunk:'100M',maxRate:'100M',threads:'10'})");
//                "util.importTable('"+filePath+"', {table: '"+tableName+"',linesTerminatedBy:'\\n',fieldsTerminatedBy:'"+delimiter+"',bytesPerChunk:'100M',maxRate:'100M',threads:'10'})");
    }
    private String completeLoadCommandCli(String com,String filePath, String tableName, String delimiter){
//mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
        return com.replace("?",
                "load data local infile '"+filePath+"' into table "+tableName+" fields terminated by '"+delimiter+"';");
    }

    public String exportCommand(DatabaseConfig config,AttachConfig attachConfig) {
        if(!StringUtil.isNullOrEmpty(attachConfig.getTempTableName()) && !StringUtil.isNullOrEmpty(attachConfig.getTempTableSQL())){
            String mysqlsh = getConnectCommand(config, "mysql");
            String exportStr = "";
            exportStr += mysqlsh.replace("?","insert into "+attachConfig.getTempTableName()+" "+attachConfig.getTempTableSQL());
            exportStr += "\n"+completeExportCommand(getConnectCommand(config,"mysqlsh"),attachConfig.getLocalCsvPath(),attachConfig.getTempTableName(),attachConfig.getDelimiter());
//            exportStr += "\n"+mysqlsh.replace("?","drop table "+attachConfig.getTempTableName());
            return exportStr;
        }else{
            return completeExportCommand(getConnectCommand(config,"mysqlsh"),attachConfig.getLocalCsvPath(),attachConfig.getRealSourceTableName(),attachConfig.getDelimiter());
        }

    }

    public String loadCommand(DatabaseConfig config,AttachConfig attachConfig) {
        return completeLoadCommandCli(getConnectCommand(config,"mysql"),attachConfig.getLocalCsvPath(),attachConfig.getCloneTableName(),attachConfig.getDelimiter());
//        return completeLoadCommand(getConnectCommand(config,"mysqlsh"),attachConfig.getLocalCsvPath(),attachConfig.getCloneTableName(),attachConfig.getDelimiter());
    }



}
