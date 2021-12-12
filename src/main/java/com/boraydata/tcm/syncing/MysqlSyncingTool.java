package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.mapping.DataMappingSQLTool;
import com.boraydata.tcm.utils.StringUtil;


/** Export and Load Table Data in MySQL,by shell statements.
 * @author bufan
 * @data 2021/9/24
 */
public class MysqlSyncingTool implements SyncingTool {
    // 1.0 this way to slow
    //-- export
    //mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem_1 limit 10"  | sed 's/"/""/g;s/\t/,/g;s/\n//g' > /usr/local/lineitem_1_limit_10.csv
    //-- load
    //mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
    // -sN  s: Produce less output  N: Not write column names in results
    // https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_skip-column-names

    // 2.0 use MySQL-Shell to export and load
    // time mysqlsh -h192.168.120.68 -P3306 -uroot -pMyNewPass4! --database test_db -e "shell.getSession().runSql(\"set foreign_key_checks=0;\");"
    //-- export
    //mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem_mysql','/usr/local/test.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
    //-- load
    //mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.importTable('/usr/local/test.csv', {table: 'lineitem_mysql',linesTerminatedBy:'\n',fieldsTerminatedBy:',',bytesPerChunk:'500M'})"


    @Override
    public String exportFile(TableCloneManageContext tcmContext) {
        Table table = tcmContext.getFinallySourceTable();
        boolean tempFlag = (tcmContext.getTempTable() != null);
        boolean hudiFlag = DataSourceType.HUDI.equals(tcmContext.getCloneConfig().getDataSourceType());
        String tempMappingSQL = "";
        if(Boolean.TRUE.equals(tempFlag))
            tempMappingSQL = DataMappingSQLTool.getMappingDataSQL(tcmContext.getSourceTable(),tcmContext.getCloneConfig().getDataSourceType());
        String csvPath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        return exportCommand(tcmContext.getSourceConfig(),table,csvPath,delimiter,tempFlag,tempMappingSQL,hudiFlag);
    }

    private String exportCommand(DatabaseConfig config,Table table, String csvPath,String delimiter,boolean tempFlag,String tempMappingSQL,boolean hudiFlag) {
        String exportStr = "";
        String tableName = table.getTablename();
        // Write data to temp table
        if(Boolean.TRUE.equals(tempFlag))
            exportStr += getConnectCommand(config, "mysql").replace("?","insert into "+table.getTablename()+" "+tempMappingSQL)+"\n";

        exportStr += completeExportCommand(getConnectCommand(config,"mysqlsh"),csvPath,tableName,delimiter,hudiFlag)+"\n";
        // drop temp table
        if(Boolean.TRUE.equals(tempFlag))
            exportStr += getConnectCommand(config, "mysql").replace("?","drop table "+tableName)+"\n";
        return exportStr;
    }

    @Override
    public String loadFile(TableCloneManageContext tcmContext) {
        String csvPath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTablename();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        return loadCommand(tcmContext.getCloneConfig(), csvPath,tablename,delimiter);
    }

    public String loadCommand(DatabaseConfig config, String csvPath,String tablename,String delimiter) {
        return completeLoadCommandCli(getConnectCommand(config,"mysql"),csvPath,tablename,delimiter);
//        return completeLoadCommand(getConnectCommand(config,"mysqlsh"),attachConfig.getLocalCsvPath(),attachConfig.getCloneTableName(),attachConfig.getDelimiter());
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

    private String completeExportCommand(String conCommand,String filePath, String tableName,String delimiter,boolean hudiFlag){
        if(hudiFlag)
            return conCommand.replace("?",
                "util.exportTable('"+tableName+"','"+filePath+"',{linesTerminatedBy:'\\n',fieldsTerminatedBy:'"+delimiter+"',fieldsOptionallyEnclosed:true,fieldsEnclosedBy:'\\\"',fieldsEscapedBy:'\\\\\\'})");
        else
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



}
