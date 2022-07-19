package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.mysql.cj.jdbc.ClientPreparedStatement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

import static com.boraydata.cdc.tcm.utils.StringUtil.escapeJava;

/**
 * @author : bufan
 * @date : 2022/7/18
 */
public class RpdSqlSyncingTool implements SyncingTool  {


    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
//        return generateExportSQLByShell(tcmContext);
        return generateExportSQLByMySQLShell(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
//        System.out.println("generateLoadSQLByMySQLShell:\n"+generateLoadSQLByMySQLShell(tcmContext)+"\n");
//        System.out.println("generateLoadSQLByShell:\n"+generateLoadSQLByShell(tcmContext)+"\n");
        return generateLoadSQLByMySQLShell(tcmContext);
//        return generateLoadSQLByShell(tcmContext);
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
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;

//        return LoadDataToMySQLByJDBC5(tcmContext);
//        return LoadDataToMySQLByJDBC8(tcmContext);
    }

    // ================================================   MySQL Syncing Common   ========================================================
    /**
     * Generate commands that both mysql and mysqlsh can use
     *
     * @Param command : mysqlsh
     * @Return: String : mysqlsh -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     *
     * @Param command : mysql
     * @Return: String : mysql -h 127.0.0.1 -P 3306 -uroot  -proot --database test_db
     */
    private String getConnectCommand(DatabaseConfig config, String command){
        return String.format("%s -h %s -P %s -u %s -p%s --database %s ",
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
     *      Although Sed is very powerful, but using Sed to format the query results will have many disadvantages.
     *      1. The query results cannot display LINE BREAKS in the table data normally
     *      2. The query results cannot distinguished the string type or format type , e.g. "   "(format type) and "\t"(string type) are displayed as \t
     *      3. The query results is default stored in the memory. If the data is too large, it will cause the machine memory overflow error.
     *          If you write the file first and then format it with Sed, there will be a lot of performance overhead.
     * e.g. {@link #replaceExportStatementShell}
     * @author: bufan
     */
    public String generateExportSQLByShell(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        boolean tempTableIsNull = Objects.isNull(tempTable);

        String tableName = tempTableIsNull?sourceTable.getTableName():tempTable.getTableName();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();

        StringBuilder sb = new StringBuilder();

        String connectCommand = getConnectCommand(tcmContext.getSourceConfig(), "mysql");


        // Write data to temp table
        // -e "insert into temp_table select * from lineitem"
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append("-e \"INSERT INTO ").append(tableName).append(" ").append(tcmContext.getTempTableSelectSQL()).append(";\"\n");

        // Export data from table
        sb.append(replaceExportStatementShell(connectCommand,tableName,csvPath,delimiter,lineSeparate,quote,escape)).append("\n");

        // drop temp table
        // -e "drop table if exists temp_table"
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append("-e \"DROP TABLE IF EXISTS ").append(tableName).append(";\"\n");

        String exportShell = sb.toString();
        tcmContext.setExportShellContent(exportShell);
        return exportShell;
    }

    // sed -r "s/a/b/g;s/b/c/g" == sed -r -e "s/a/b/g" -e "s/b/c/g"
    private String replaceExportStatementShell(String con, String tableName, String filePath, String delimiter,String lineSeparate,String quote,String escape){
        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);
        if(Arrays.asList("\\","|","+","?","'","`","<",">","b","B","w","W").contains(delimiter))
            delimiter = "\\"+delimiter;
        if(Arrays.asList("\\","|","+","?","'","`","<",">","b","B","w","W").contains(lineSeparate))
            lineSeparate = "\\"+lineSeparate;
        if(Arrays.asList("\\","|","+","?","'","`","<",">","b","B","w","W").contains(quote))
            quote = "\\"+quote;
        if(Arrays.asList("\\","|","+","?","'","`","<",">","b","B","w","W").contains(escape))
            escape = "\\"+escape;


        StringBuilder sb = new StringBuilder(con).append("-sN -e \"SELECT * FROM ").append(escapeJava(tableName,"\"")).append("\"");
        StringBuilder sedCommand = new StringBuilder();
        if(StringUtil.nonEmpty(escape)){
            sedCommand.append("s/").append(escape).append("/").append(escape).append(escape).append("/g;");
            if(StringUtil.nonEmpty(delimiter) && !delimiter.equals(escape))
                sedCommand.append("s/").append(delimiter).append("/").append(escape).append(delimiter).append("/g;");
            if(StringUtil.nonEmpty(lineSeparate) && !lineSeparate.equals(escape))
                sedCommand.append("s/").append(lineSeparate).append("/").append(escape).append(lineSeparate).append("/g;");
            if(StringUtil.nonEmpty(quote) && !quote.equals(escape))
                sedCommand.append("s/").append(quote).append("/").append(escape).append(quote).append("/g;");
        }
        if(StringUtil.nonEmpty(quote))
            sedCommand.append("s/^/").append(quote).append("/g;").append("s/$/").append(quote).append("/g;");

        sedCommand.append("s/\\t/").append(quote).append(delimiter).append(quote).append("/g;");

//        if(StringUtil.nonEmpty(lineSeparate))
//            sb.append("s/\\n/").append(lineSeparate).append("/g;");
        String command = sedCommand.toString().replaceAll("s/\\\\\\\\/","s/\\\\\\\\\\\\/");
        command = escapeJava(command,"\"");

        return sb.append(" | sed -r \"").append(command).append("\" > ").append(filePath).toString();
    }


    /**
     * Load By shell,if have TempTable,will get mapping table data in TempTable.
     * @return mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db --local-infile=ON -e "load data local infile '/usr/local/lineitem.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
     * --local-infile=ON:
     *      @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_local_infile"></a>
     * @author: bufan
     */
    public String generateLoadSQLByShell(TableCloneManagerContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();

        StringBuilder stringBuilder = new StringBuilder();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig(),"mysql");
        String loadStatementShell = replaceLoadStatementShell(tablename,csvPath, delimiter, lineSeparate, quote, escape).replaceAll("\\\\","\\\\\\\\");
        stringBuilder.append(connectCommand).append("--local-infile=ON -e ").append("\"").append(escapeJava(loadStatementShell,"\"")).append("\"");

        tcmContext.setLoadShellContent(stringBuilder.toString());
        return stringBuilder.toString();
    }

    /**
     * @return: load data local infile './test.csv' into table lineitem fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n'
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/load-data.html"></a>
     */
    private String replaceLoadStatementShell(String tableName,String filePath,  String delimiter,String lineSeparate,String quote,String escape){
        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);
        StringBuilder sb = new StringBuilder("LOAD DATA LOCAL INFILE '").append(escapeJava(filePath,"'")).append("' INTO TABLE ").append(tableName);
        if(StringUtil.nonEmpty(delimiter)) {
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equalsIgnoreCase(delimiter))
                sb.append(" FIELDS TERMINATED BY x'07'");
            else
                sb.append(" FIELDS TERMINATED BY '").append(delimiter).append("'");
        }
        if(StringUtil.nonEmpty(quote))
            sb.append(" ENCLOSED BY '").append(escapeJava(quote,"'")).append("'");
        if(StringUtil.nonEmpty(escape))
            sb.append(" ESCAPED BY '").append(escapeJava(escape,"'")).append("'");
        if(StringUtil.nonEmpty(lineSeparate))
            sb.append(" LINES TERMINATED BY '").append(escapeJava(lineSeparate,"'")).append("'");
        return sb.toString();
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
     * {@link #replaceExportStatementMySQLShell}
     * @return: mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem','/usr/local/lineitem.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
     * @author: bufan
     */
    public String generateExportSQLByMySQLShell(TableCloneManagerContext tcmContext){

        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        DatabaseConfig config = tcmContext.getSourceConfig();
        boolean tempTableIsNull = Objects.isNull(tempTable);
        String tableName = tempTableIsNull?sourceTable.getTableName():tempTable.getTableName();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        StringBuilder sb = new StringBuilder();
        String connectCommand = getConnectCommand(config, "mysqlsh");
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("--sql -e \"INSERT INTO %s %s;\"",tableName,tcmContext.getTempTableSelectSQL())).append("\n");
        boolean coverNull = false;
        for (Column col : tcmContext.getSourceTable().getColumns())
            coverNull = coverNull||col.getNullable();
        if(coverNull)
            coverNull = !(DataSourceEnum.MYSQL.equals(tcmContext.getCloneConfig().getDataSourceEnum()) || DataSourceEnum.RPDSQL.equals(tcmContext.getCloneConfig().getDataSourceEnum()));
        sb.append(replaceExportStatementMySQLShell(connectCommand,tableName,csvPath,delimiter,lineSeparate,quote,escape,coverNull)).append("\n");
        // drop temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("--sql -e \"DROP TABLE IF EXISTS %s;\"",tableName)).append("\n");
        String exportMySQLShell = sb.toString();
        tcmContext.setExportShellContent(exportMySQLShell);
        return exportMySQLShell;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-table-export.html#mysql-shell-utilities-table-export-run">mysql-shell-utilities-table-export</a>
     * @author: bufan
     */
    private String replaceExportStatementMySQLShell(String conCommand,String tableName,String filePath,String delimiter,String lineSeparate,String quote,String escape,boolean coverNull){

        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);

        StringBuilder exportTable = new StringBuilder("util.exportTable('").append(tableName).append("','").append(filePath).append("',{");
        if(StringUtil.nonEmpty(delimiter)) {
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                exportTable.append("fieldsTerminatedBy:'\\007',");
            else
                exportTable.append("fieldsTerminatedBy:'").append(delimiter).append("',");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            exportTable.append("linesTerminatedBy:'").append(lineSeparate).append("',");
        if(StringUtil.nonEmpty(quote))
            exportTable.append("fieldsOptionallyEnclosed:false,fieldsEnclosedBy:'").append(quote).append("',");
        if(StringUtil.nonEmpty(escape))
            exportTable.append("fieldsEscapedBy:'").append(escape).append("',");
        if(exportTable.lastIndexOf(",") == exportTable.length()-1)
            exportTable.deleteCharAt(exportTable.length()-1);
        String mysqlShellCommand = escapeJava(exportTable.toString(),"\"").replaceAll("\\\\\\\\","\\\\\\\\\\\\");

        StringBuilder sb = new StringBuilder(conCommand).append(" -e ").append("\"").append(mysqlShellCommand).append("})\"\n");

        if(coverNull)
            sb.append("sed -i -r \"s/\\\\\\N//g\" ").append(filePath).append("\n");
        return sb.toString();
    }

    /**
     * Load By mysql-shell,if have TempTable,will get mapping table data in TempTable.
     * {@link #replaceLoadStatementMySQLShell}
     * @return: mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.importTable('/usr/local/lineitem.csv', {table: 'lineitem',linesTerminatedBy:'\n',fieldsTerminatedBy:',',bytesPerChunk:'500M'})"
     * @author: bufan
     */
    public String generateLoadSQLByMySQLShell(TableCloneManagerContext tcmContext){
        DatabaseConfig config = tcmContext.getCloneConfig();
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String loadContent = replaceLoadStatementMySQLShell(getConnectCommand(config,"mysqlsh"),tablename,csvPath,delimiter,lineSeparate,quote,escape);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-utilities-parallel-table.html#mysql-shell-utilities-parallel-table-options">mysql-shell-utilities-table-import</a>
     * @author: bufan
     */
    private String replaceLoadStatementMySQLShell(String con, String tableName,String filePath, String delimiter,String lineSeparate,String quote,String escape){

        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);

        StringBuilder importTable = new StringBuilder("util.importTable('").append(filePath).append("', {");
        StringBuilder sb = new StringBuilder(con).append("-e \"util.importTable('").append(filePath).append("', {");
        importTable.append("characterSet:'utf8mb4',");
        if(StringUtil.nonEmpty(tableName))
            importTable.append("table:'").append(tableName).append("',");
        if(StringUtil.nonEmpty(delimiter)) {
//            sb.append("fieldsTerminatedBy:'").append(delimiter).append("',");
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                importTable.append("fieldsTerminatedBy:'\\007',");
            else
                importTable.append("fieldsTerminatedBy:'").append(delimiter).append("',");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            importTable.append("linesTerminatedBy:'").append(lineSeparate).append("',");
        if(StringUtil.nonEmpty(quote))
            importTable.append("fieldsEnclosedBy:'").append(quote).append("',");
        if(StringUtil.nonEmpty(escape))
            importTable.append("fieldsEscapedBy:'").append(escape).append("',");
        importTable.append("bytesPerChunk:'300M',maxRate:'500M',threads:'8'})");
        String mysqlShellCommand = escapeJava(importTable.toString(),"\"").replaceAll("\\\\\\\\","\\\\\\\\\\\\");
        return new StringBuilder(con).append("-e \"").append(mysqlShellCommand).append("\"").toString();
    }

    // ================================================   JDBC   ========================================================
    /**
     * @see <a href="https://stackoverflow.com/questions/53156112/how-to-allow-load-data-local-infile-on-mysql-connector-java">MySQL 8 Load data infile</a>
     */
    public String generateLoadSQLByJDBC(TableCloneManagerContext tcmContext){
        String filePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String sql = replaceLoadStatementShell(tableName,filePath,delimiter,lineSeparate,quote,escape);
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
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }
}
