package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.mysql.cj.jdbc.ClientPreparedStatement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * Export and Load Table Data by MySQL.
 *
 * hudiFlag:
 *      Spark parsing CSV If you need to use Escape characters, you need to enclose the field,
 *      that is, you can use Escape only in combination with the Quote attribute.
 * @see <a href="https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html"></a>
 *  e.g.
 *      delimiter:,
 *      escape:\
 *      Source Table Data:{test,test-delimiter:,,test-escape:\,test-quote:"}
 *      default CSV File => test,test-delimiter:\,,test-escape:\\,test-quote:"
 *      But Spark need => "test","test-delimiter:,","test-escape:\\","test-quote:\""
 *
 *      val df = spark.read.option("delimiter", ",").option("escape", "\\").option("quote", "\"").csv(path)
 *      df.show()
 *   each column of Csv File will be wrapped with the content of Quote. If the data contains Escape or Quote
 *    you need to add Escape in front of it to Escape.
 *
 * @author bufan
 * @date 2021/9/24
 */

public class MysqlSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
//        return generateExportSQLByShell(tcmContext);
        return generateExportSQLByMysqlShell(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
//        return generateLoadSQLByMysqlShell(tcmContext);
//        return generateLoadSQLByShell(tcmContext);
        return generateLoadSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeExport(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
    }

    @Override
    public Boolean executeLoad(TableCloneManagerContext tcmContext) {
//        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
//        if(tcmContext.getTcmConfig().getDebug())
//            System.out.println(outStr);
//        return true;

//        return LoadDataToMySQLByJDBC5(tcmContext);
        return LoadDataToMySQLByJDBC8(tcmContext);
    }

    /**
     * Generate commands that both mysql and mysqlsh can use
     *
     * @Param command : mysqlsh
     * @Return: String : mysqlsh -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     * @Param command : mysql
     * @Return: String : mysql -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     */
    private String getConnectCommand(DatabaseConfig config, String command){
        return String.format("%s -h %s -P %s -u%s -p%s --database %s ",
                command,
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabaseName());
    }

    // ================================================   Shell   ========================================================

    /**
     * Export By shell,if have TempTable,will get mapping table data in TempTable.
     *
     * @return mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem" | sed 's/\\/\\\\/g;s/,/\\/g,;s/\t/,/g;s/\n//g' > /usr/lineitem.csv
     *   -sN:
     *      s: Produce less output
     *      N: Not write column names in results
     *      @see <a href="https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_skip-column-names"></a>
     *   Sed command manual:
     *      @see <a href="https://www.gnu.org/software/sed/manual/sed.html#sed-commands-list"></a>
     * e.g. {@link #replaceExportStatementShell}
     * @author: bufan
     */
    private String generateExportSQLByShell(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        DatabaseConfig config = tcmContext.getSourceConfig();
        boolean tempTableIsNull = Objects.isNull(tempTable);
        boolean hudiFlag = DataSourceEnum.HUDI.equals(tcmContext.getCloneConfig().getDataSourceEnum());

        String tableName = tempTableIsNull?sourceTable.getTableName():tempTable.getTableName();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        StringBuilder sb = new StringBuilder();
        String connectCommand = getConnectCommand(config, "mysql");

//        String tempMappingSQL = "";
//        if(Boolean.FALSE.equals(tempTableIsNull))
//            tempMappingSQL = DataMappingSQLTool.getMappingDataSQL(tcmContext.getSourceTable(),tcmContext.getCloneConfig().getDataSourceEnum());


        // Write data to temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("-e \"insert into %s %s;\"",tableName,tcmContext.getTempTableSelectSQL())).append("\n");

        sb.append(replaceExportStatementShell(connectCommand,csvPath,tableName,delimiter,hudiFlag)).append("\n");
        // drop temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("-e \"drop table if exists %s;\"",tableName)).append("\n");
        String exportShell = sb.toString();
        tcmContext.setExportShellContent(exportShell);
        return exportShell;
    }

    /**
     * -sN -e "select * from lineitem"  | sed 's/\\/\\\\/g;s/,/\\/g,;s/\t/,/g;s/\n//g' > /usr/local/lineitem.csv
     *  default query data :
     *      1       24027   1534    5       24.00   22824.48        0.10    0.04    N       O       1996-03-30      1996-03-14      1996-04-01      NONE    FOB      pending foxes. slyly re
     *      3       19036   6540    2       49.00   46796.47        0.10    0.00    R       F       1993-11-09      1993-12-20      1993-11-24      TAKE BACK RETURN        RAIL     unusual accounts.
     *
     *  sed format query data:
     *      1,24027,1534,5,24.00,22824.48,0.10,0.04,N,O,1996-03-30,1996-03-14,1996-04-01,NONE,FOB, pending foxes. slyly re
     *      3,19036,6540,2,49.00,46796.47,0.10,0.00,R,F,1993-11-09,1993-12-20,1993-11-24,TAKE BACK RETURN,RAIL, unusual accounts. eve
     *
     * echo -e 'a\tb\tc' | sed 's/"/""/g;s/^/"/;s/$/"/;s/\t/","/g;s/\n//g'   =>   "a","b","c"
     */
    private String replaceExportStatementShell(String con, String filePath, String tableName, String delimiter,boolean hudiFlag){
        StringBuilder sb = new StringBuilder(con);
        if(hudiFlag)
            sb.append("-sN -e \"select * from ").append(tableName).append("\" | sed 's/\\\\/\\\\\\\\/g;s/\"/\\\"/g;s/"+delimiter+"/\\"+delimiter+"/g;s/\\t/\""+ delimiter +"\"/g;s/^/\"/;s/$/\"/;s/\\n//g' > ").append(filePath);
        else
            sb.append("-sN -e \"select * from ").append(tableName).append("\" | sed 's/\\\\/\\\\\\\\/g;s/"+delimiter+"/\\"+delimiter+"/g;s/\\t/"+ delimiter +"/g;s/\\n//g' > ").append(filePath);
        return sb.toString();
    }


    /**
     * Load By shell,if have TempTable,will get mapping table data in TempTable.
     * @return mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db --local-infile=ON -e "load data local infile '/usr/local/lineitem.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
     * --local-infile=ON:
     *      @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_local_infile"></a>
     * @author: bufan
     */
    private String generateLoadSQLByShell(TableCloneManagerContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        StringBuilder stringBuilder = new StringBuilder();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig(),"mysql");
        String loadStatementShell = replaceLoadStatementShell(csvPath, tablename, delimiter);
        stringBuilder.append(connectCommand).append("--local-infile=ON -e ").append("\"").append(loadStatementShell).append("\"");

        tcmContext.setLoadShellContent(stringBuilder.toString());
        return stringBuilder.toString();
    }

    /**
     * @return: load data local infile './test.csv' into table lineitem fields terminated by ',' lines terminated by '\n'
     */
    private String replaceLoadStatementShell(String filePath, String tableName, String delimiter){
        return String.format("load data local infile '%s' into table %s fields terminated by '%s' lines terminated by '%s'",
                filePath,
                tableName,
                delimiter,
                "\\n");
    }


    // ================================================   MySQL - Shell   ========================================================
    /**
     * MySQL-Shell is an official tool, supported util.exportTable() and util.importTable() in 8.0.22 version.
     * @see <a href="https://dev.mysql.com/doc/relnotes/mysql-shell/8.0/en/news-8-0-22.html"></a>
     * more utilities can to:
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities.html"></a>
     *
     * the connect mode defaule JavaScript,you can execute SQL Statement like this:
     *      mysqlsh -h192.168.120.68 -P3306 -uroot -pMyNewPass4! --database test_db -e "shell.getSession().runSql(\"set foreign_key_checks=0;\");"
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-batch-code-execution.html"></a>
     * Or check the mode by '--sql'
     *      mysqlsh -h192.168.120.68 -P3306 -uroot -pMyNewPass4! --database test_db --sql -e "show tables"
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysqlsh.html#option_mysqlsh_sql"></a>
     */

    /**
     * Export By mysql-shell,if have TempTable,will get mapping table data in TempTable.
     * {@link #replaceExportStatementMysqlShell}
     * @return: mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem','/usr/local/lineitem.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
     * @author: bufan
     */
    private String generateExportSQLByMysqlShell(TableCloneManagerContext tcmContext){

        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        DatabaseConfig config = tcmContext.getSourceConfig();
        boolean tempTableIsNull = Objects.isNull(tempTable);
        boolean hudiFlag = DataSourceEnum.HUDI.equals(tcmContext.getCloneConfig().getDataSourceEnum());

        String tableName = tempTableIsNull?sourceTable.getTableName():tempTable.getTableName();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        StringBuilder sb = new StringBuilder();
        String connectCommand = getConnectCommand(config, "mysqlsh");
        // Write data to temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("--sql -e \"insert into %s %s;\"",tableName,tcmContext.getTempTableSelectSQL())).append("\n");

        sb.append(replaceExportStatementMysqlShell(connectCommand,csvPath,tableName,delimiter,hudiFlag)).append("\n");
        // drop temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("--sql -e \"drop table if exists %s;\"",tableName)).append("\n");
        String exportMysqlShell = sb.toString();
        tcmContext.setExportShellContent(exportMysqlShell);
        return exportMysqlShell;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-table-export.html#mysql-shell-utilities-table-export-run">mysql-shell-utilities-table-export</a>
     * @author: bufan
     */
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

    /**
     * Load By mysql-shell,if have TempTable,will get mapping table data in TempTable.
     * {@link #replaceLoadStatementMysqlShell}
     * @return: mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.importTable('/usr/local/lineitem.csv', {table: 'lineitem',linesTerminatedBy:'\n',fieldsTerminatedBy:',',bytesPerChunk:'500M'})"
     * @author: bufan
     */
    private String generateLoadSQLByMysqlShell(TableCloneManagerContext tcmContext){
        DatabaseConfig config = tcmContext.getCloneConfig();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();

        String delimiter = tcmContext.getTcmConfig().getDelimiter();

        String loadContent = replaceLoadStatementMysqlShell(getConnectCommand(config,"mysqlsh"),csvPath,tablename,delimiter);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-parallel-table.html#mysql-shell-utilities-parallel-table-options">mysql-shell-utilities-table-import</a>
     * @author: bufan
     */
    private String replaceLoadStatementMysqlShell(String con,String filePath, String tableName, String delimiter){
        String format = String.format("-e \"util.importTable('%s', {characterSet:'utf8mb4',table:'%s',fieldsTerminatedBy:'%s',linesTerminatedBy:'%s',bytesPerChunk:'300M',maxRate:'500M',threads:'8'})\"",
                filePath,
                tableName,
                delimiter,
                "\\n");
        return con+format;
    }

    // ================================================   JDBC   ========================================================
    /**
     * @see <a href="https://stackoverflow.com/questions/53156112/how-to-allow-load-data-local-infile-on-mysql-connector-java">MySQL 8 Load data infile</a>
     */
    private String generateLoadSQLByJDBC(TableCloneManagerContext tcmContext){
        String filePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String sql = replaceLoadStatementShell(filePath,tableName,delimiter);
        tcmContext.setLoadShellContent(sql);
        return "# MySQL Load Data By JDBC,SQL Statements : "+sql;
    }

    private Boolean LoadDataToMySQLByJDBC5(TableCloneManagerContext tcmContext){
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

    /**
     * @see <a href="https://blog.csdn.net/weixin_44241785/article/details/108474544"></a>
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html"></a>
     * @see <a href="https://stackoverflow.com/questions/3627537/is-a-load-data-without-a-file-i-e-in-memory-possible-for-mysql-and-java"></a>
     */
    private Boolean LoadDataToMySQLByJDBC8(TableCloneManagerContext tcmContext){
        String sql = tcmContext.getLoadShellContent();

        String csvFilePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
//        String sql = "load data local infile '' into table test_table";
        sql = sql.replaceAll(csvFilePath,"");
        System.out.println(sql);
        File file = FileUtil.getFile(csvFilePath);


        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getCloneConfig());
                PreparedStatement statement = conn.prepareStatement(sql)
        ){
            if(statement.isWrapperFor(com.mysql.cj.jdbc.JdbcPreparedStatement.class)){
                ClientPreparedStatement mysql8Statement = statement.unwrap(com.mysql.cj.jdbc.ClientPreparedStatement.class);
                mysql8Statement.setLocalInfileInputStream(new FileInputStream(file));
                mysql8Statement.execute();
//                mysql8Statement.executeUpdate();
                return true;
            }
//            else if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)){
//                PrepareStatement mysql5Statement = statement.unwrap(com.mysql.jdbc.PrepareStatement.class);
////                mysql5Statement.setLocalInfileInputStream(FileUtil.getFile());
//                mysql5Statement.executeUpdate();
//                return true;
//            }else
                return false;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

}
