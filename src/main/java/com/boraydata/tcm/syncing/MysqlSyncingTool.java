package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.mapping.DataMappingSQLTool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/** Export and Load Table Data in MySQL,by shell statements.
 * @author bufan
 * @data 2021/9/24
 */
// =======================================================   Shell
//-- export
//mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem"  | sed 's/"/""/g;s/\t/,/g;s/\n//g' > /usr/local/lineitem.csv
// -sN  s: Produce less output  N: Not write column names in results
// https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_skip-column-names
// Sed manual
// https://www.gnu.org/software/sed/manual/sed.html#sed-commands-list
//
//-- load
//mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"


// =======================================================   MySQL-Shell
// time mysqlsh -h192.168.120.68 -P3306 -uroot -pMyNewPass4! --database test_db -e "shell.getSession().runSql(\"set foreign_key_checks=0;\");"
//-- export
//mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem','/usr/local/lineitem.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
//-- load
//mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.importTable('/usr/local/lineitem.csv', {table: 'lineitem',linesTerminatedBy:'\n',fieldsTerminatedBy:',',bytesPerChunk:'500M'})"


public class MysqlSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManageContext tcmContext) {
//        return generateExportSQLByShell(tcmContext);
        return generateExportSQLByMysqlShell(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManageContext tcmContext) {
//        return generateLoadSQLByMysqlShell(tcmContext);
        return generateLoadSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeExport(TableCloneManageContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
    }

    @Override
    public Boolean executeLoad(TableCloneManageContext tcmContext) {
//        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellContent(),tcmContext.getTcmConfig().getDebug());
//        if(tcmContext.getTcmConfig().getDebug())
//            System.out.println(outStr);
//        return true;

        return LoadDataToMySQLByJDBC(tcmContext);
    }

    /**
     * Generate commands that both mysql and mysqlsh can use
     *
     * @Param command : mysql
     * @Return: String : mysql -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     *
     * @Param command : mysqlsh
     * @Return: String : mysqlsh -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     */
    private String getConnectCommand(DatabaseConfig config,String command){
        return String.format("%s -h %s -P %s -u%s -p%s --database %s ",
                command,
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabasename());
    }

    // ================================================   Shell   ========================================================

    private String generateExportSQLByShell(TableCloneManageContext tcmContext){
        String tableName = tcmContext.getFinallySourceTable().getTableName();
        DatabaseConfig config = tcmContext.getSourceConfig();
        boolean tempFlag = (tcmContext.getTempTable() != null);
        boolean hudiFlag = DataSourceType.HUDI.equals(tcmContext.getCloneConfig().getDataSourceType());
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        String tempMappingSQL = "";
        if(Boolean.TRUE.equals(tempFlag))
            tempMappingSQL = DataMappingSQLTool.getMappingDataSQL(tcmContext.getSourceTable(),tcmContext.getCloneConfig().getDataSourceType());

        StringBuilder sb = new StringBuilder();
        String connectCommand = getConnectCommand(config, "mysql");
        // Write data to temp table
        if(Boolean.TRUE.equals(tempFlag))
            sb.append(connectCommand).append(String.format("-e \"insert into %s %s;\"",tableName,tempMappingSQL)).append("\n");

        sb.append(replaceExportStatementShell(connectCommand,csvPath,tableName,delimiter,hudiFlag)).append("\n");
        // drop temp table
        if(Boolean.TRUE.equals(tempFlag))
            sb.append(connectCommand).append(String.format("-e \"drop table if exists %s;\"",tableName)).append("\n");
        String exportShell = sb.toString();
        tcmContext.setExportShellContent(exportShell);
        return exportShell;
    }

    /**
     * delimiter: ,
     * Default Escape: \
     * -sN -e "select * from lineitem"  | sed 's/\\/\\\\/g;s/,/\\/g,;s/\t/,/g;s/\n//g' > /usr/local/lineitem.csv
     * defalt :
     *  1       24027   1534    5       24.00   22824.48        0.10    0.04    N       O       1996-03-30      1996-03-14      1996-04-01      NONE    FOB      pending foxes. slyly re
     *  3       19036   6540    2       49.00   46796.47        0.10    0.00    R       F       1993-11-09      1993-12-20      1993-11-24      TAKE BACK RETURN        RAIL     unusual accounts.
     *
     *  sed format:
     * 1,24027,1534,5,24.00,22824.48,0.10,0.04,N,O,1996-03-30,1996-03-14,1996-04-01,NONE,FOB, pending foxes. slyly re
     * 3,19036,6540,2,49.00,46796.47,0.10,0.00,R,F,1993-11-09,1993-12-20,1993-11-24,TAKE BACK RETURN,RAIL, unusual accounts. eve
     *
     * sed 's/"/""/g;s/^/"/;s/$/"/;s/\t/","/g;s/\n//g'   =>   "a","b","c"
     */
    private String replaceExportStatementShell(String con, String filePath, String tableName, String delimiter,boolean hudiFlag){
        StringBuilder sb = new StringBuilder(con);
        if(hudiFlag)
            sb.append("-sN -e \"select * from ").append(tableName).append("\" | sed 's/\\\\/\\\\\\\\/g;s/\"/\\\"/g;s/"+delimiter+"/\\"+delimiter+"/g;s/\\t/\""+ delimiter +"\"/g;s/^/\"/;s/$/\"/;s/\\n//g' > ").append(filePath);
        else
            sb.append("-sN -e \"select * from ").append(tableName).append("\" | sed 's/\\\\/\\\\\\\\/g;s/"+delimiter+"/\\"+delimiter+"/g;s/\\t/"+ delimiter +"/g;s/\\n//g' > ").append(filePath);
        return sb.toString();
    }


    //mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db --local-infile=ON -e "load data local infile '/usr/local/lineitem.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
    private String generateLoadSQLByShell(TableCloneManageContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        // https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_local_infile
        String loadContent = replaceLoadStatementShell(getConnectCommand(tcmContext.getCloneConfig(),"mysql")+"--local-infile=ON -e \"",csvPath,tablename,delimiter)+"\"";
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    // load data local infile './test.csv' into table lineitem fields terminated by ',' lines terminated by '\n'
    private String replaceLoadStatementShell(String con, String filePath, String tableName, String delimiter){
        String format = String.format("load data local infile '%s' into table %s fields terminated by '%s' lines terminated by '%s'",
                filePath,
                tableName,
                delimiter,
                "\\n");
        return con+format;
    }


    // ================================================   MySQL - Shell   ========================================================

    private String generateExportSQLByMysqlShell(TableCloneManageContext tcmContext){
        String tableName = tcmContext.getFinallySourceTable().getTableName();
        DatabaseConfig config = tcmContext.getSourceConfig();
        boolean tempFlag = (tcmContext.getTempTable() != null);
        boolean hudiFlag = DataSourceType.HUDI.equals(tcmContext.getCloneConfig().getDataSourceType());
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        String tempMappingSQL = "";
        if(Boolean.TRUE.equals(tempFlag))
            tempMappingSQL = DataMappingSQLTool.getMappingDataSQL(tcmContext.getSourceTable(),tcmContext.getCloneConfig().getDataSourceType());

        StringBuilder sb = new StringBuilder();
        String connectCommand = getConnectCommand(config, "mysqlsh");
        // Write data to temp table
        if(Boolean.TRUE.equals(tempFlag))
            sb.append(connectCommand).append(String.format("--sql -e \"insert into %s %s;\"",tableName,tempMappingSQL)).append("\n");

        sb.append(replaceExportStatementMysqlShell(connectCommand,csvPath,tableName,delimiter,hudiFlag)).append("\n");
        // drop temp table
        if(Boolean.TRUE.equals(tempFlag))
            sb.append(connectCommand).append(String.format("--sql -e \"drop table if exists %s;\"",tableName)).append("\n");
        String exportMysqlShell = sb.toString();
        tcmContext.setExportShellContent(exportMysqlShell);
        return exportMysqlShell;
    }

    // https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-table-export.html#mysql-shell-utilities-table-export-options
    private String replaceExportStatementMysqlShell(String conCommand,String filePath, String tableName,String delimiter,boolean hudiFlag){
        String statements;
        if(hudiFlag)
            statements = String.format(" -e \"util.exportTable('%s','%s',{linesTerminatedBy:'%s',fieldsTerminatedBy:'%s',fieldsOptionallyEnclosed:false,fieldsEnclosedBy:'\\\"',fieldsEscapedBy:'\\\\\\'})\"",
                    tableName,
                    filePath,
                    "\\n",
                    delimiter);
        else
            statements = String.format(" -e \"util.exportTable('%s','%s',{linesTerminatedBy:'%s',fieldsTerminatedBy:'%s'})\"",
                    tableName,
                    filePath,
                    "\\n",
                    delimiter);

        return conCommand+statements;
    }

    private String generateLoadSQLByMysqlShell(TableCloneManageContext tcmContext){
        DatabaseConfig config = tcmContext.getCloneConfig();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();

        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        String loadContent = replaceLoadStatementMysqlShell(getConnectCommand(config,"mysqlsh"),csvPath,tablename,delimiter);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    private String replaceLoadStatementMysqlShell(String con,String filePath, String tableName, String delimiter){
        String format = String.format("-e \"util.importTable('%s', {characterSet:'utf8mb4',table:'%s',fieldsTerminatedBy:'%s',linesTerminatedBy:'%s',bytesPerChunk:'300M',maxRate:'500M',threads:'8'})\"",
                filePath,
                tableName,
                delimiter,
                "\\n");
        return con+format;
    }

    // ================================================   JDBC   ========================================================

    private String generateLoadSQLByJDBC(TableCloneManageContext tcmContext){
        String filePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String sql = replaceLoadStatementShell("",filePath,tableName,delimiter);
        tcmContext.setLoadShellContent(sql);
        return "# MySQL Load Data By JDBC,SQL Statements : "+sql;
    }

    private Boolean LoadDataToMySQLByJDBC(TableCloneManageContext tcmContext){
        String sql = tcmContext.getLoadShellContent();
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getCloneConfig());
                Statement statement = conn.createStatement()
        ){
            boolean execute = statement.execute(sql);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

}
