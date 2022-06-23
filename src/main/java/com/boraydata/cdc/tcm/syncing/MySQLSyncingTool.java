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
import java.util.Objects;

/**
 * Export and Load Table Data by MySQL.
 *
 * @author bufan
 * @date 2021/9/24
 */

public class MySQLSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
//        return generateExportSQLByShell(tcmContext);
        return generateExportSQLByMySQLShell(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
//        System.out.println("generateLoadSQLByMySQLShell:\n"+generateLoadSQLByMySQLShell(tcmContext)+"\n");
//        System.out.println("generateLoadSQLByShell:\n"+generateLoadSQLByShell(tcmContext)+"\n");
//        return generateLoadSQLByMySQLShell(tcmContext);
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
    private String generateExportSQLByShell(TableCloneManagerContext tcmContext){
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
        tableName = StringUtil.escapeRegexQuoteEncode(tableName).replaceAll("\\|","\\\\|");
        delimiter = StringUtil.escapeRegexQuoteEncode(delimiter).replaceAll("\\|","\\\\|");
        lineSeparate = StringUtil.escapeRegexQuoteEncode(lineSeparate).replaceAll("\\|","\\\\|");
        quote = StringUtil.escapeRegexQuoteEncode(quote).replaceAll("\\|","\\\\|");
        escape = StringUtil.escapeRegexQuoteEncode(escape).replaceAll("\\|","\\\\|");

        StringBuilder sb = new StringBuilder(con).append("-sN -e \"SELECT * FROM ").append(tableName).append("\" | sed -r \"");
        if(StringUtil.nonEmpty(escape)){
            sb.append("s/").append(escape).append("/").append(escape).append(escape).append("/g;");
            if(StringUtil.nonEmpty(delimiter) && !delimiter.equals(escape))
                sb.append("s/").append(delimiter).append("/").append(escape).append(delimiter).append("/g;");
            if(StringUtil.nonEmpty(lineSeparate) && !lineSeparate.equals(escape))
                sb.append("s/").append(lineSeparate).append("/").append(escape).append(lineSeparate).append("/g;");
            if(StringUtil.nonEmpty(quote) && !quote.equals(escape))
                sb.append("s/").append(quote).append("/").append(escape).append(quote).append("/g;");
        }
        if(StringUtil.nonEmpty(quote))
            sb.append("s/^/").append(quote).append("/g;").append("s/$/").append(quote).append("/g;");

        sb.append("s/\\t/").append(quote).append(delimiter).append(quote).append("/g;");

//        if(StringUtil.nonEmpty(lineSeparate))
//            sb.append("s/\\n/").append(lineSeparate).append("/g;");

        return sb.append("\" > ").append(filePath).toString();
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
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();

        StringBuilder stringBuilder = new StringBuilder();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig(),"mysql");
        String loadStatementShell = replaceLoadStatementShell(tablename,csvPath, delimiter, lineSeparate, quote, escape).replaceAll("\\\\","\\\\\\\\");
        stringBuilder.append(connectCommand).append("--local-infile=ON -e ").append("\"").append(StringUtil.escapeRegexDoubleQuoteEncode(loadStatementShell)).append("\"");

        tcmContext.setLoadShellContent(stringBuilder.toString());
        return stringBuilder.toString();
    }

    /**
     * @return: load data local infile './test.csv' into table lineitem fields terminated by ',' enclosed by '"' escaped by '\\' lines terminated by '\n'
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/load-data.html"></a>
     */
    private String replaceLoadStatementShell(String tableName,String filePath,  String delimiter,String lineSeparate,String quote,String escape){
        filePath = StringUtil.escapeRegexSingleQuoteEncode(filePath);
//        tableName = StringUtil.escapeRegexSingleQuoteEncode(tableName);
        delimiter = StringUtil.escapeRegexSingleQuoteEncode(delimiter).replaceAll("\\\\","\\\\\\\\");
        lineSeparate = StringUtil.escapeRegexSingleQuoteEncode(lineSeparate);
        if(!"\\n".equals(lineSeparate))
            lineSeparate = lineSeparate.replaceAll("\\\\","\\\\\\\\");
        quote = StringUtil.escapeRegexSingleQuoteEncode(quote).replaceAll("\\\\","\\\\\\\\");
        escape = StringUtil.escapeRegexSingleQuoteEncode(escape).replaceAll("\\\\","\\\\\\\\");


        StringBuilder sb = new StringBuilder("LOAD DATA LOCAL INFILE '").append(filePath).append("' INTO TABLE ").append(tableName);

        if(StringUtil.nonEmpty(delimiter)) {
            if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equalsIgnoreCase(delimiter))
                sb.append(" FIELDS TERMINATED BY x'07'");
            else
                sb.append(" FIELDS TERMINATED BY '").append(delimiter).append("'");
        }
        if(StringUtil.nonEmpty(quote))
            sb.append(" ENCLOSED BY '").append(quote).append("'");
        if(StringUtil.nonEmpty(escape))
            sb.append(" ESCAPED BY '").append(escape).append("'");
        if(StringUtil.nonEmpty(lineSeparate))
            sb.append(" LINES TERMINATED BY '").append(lineSeparate).append("'");

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
    private String generateExportSQLByMySQLShell(TableCloneManagerContext tcmContext){

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
        // Write data to temp table
        if(Boolean.FALSE.equals(tempTableIsNull))
            sb.append(connectCommand).append(String.format("--sql -e \"INSERT INTO %s %s;\"",tableName,tcmContext.getTempTableSelectSQL())).append("\n");


        boolean coverNull = false;
        for (Column col : tcmContext.getSourceTable().getColumns())
            coverNull = coverNull||col.getNullable();
        if(coverNull)
            coverNull = !DataSourceEnum.MYSQL.equals(tcmContext.getCloneConfig().getDataSourceEnum());


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
        tableName = StringUtil.escapeRegexQuoteEncode(tableName);
        filePath = StringUtil.escapeRegexQuoteEncode(filePath);
        delimiter = StringUtil.escapeRegexQuoteEncode(delimiter);
        lineSeparate = StringUtil.escapeRegexQuoteEncode(lineSeparate);
        quote = StringUtil.escapeRegexQuoteEncode(quote);
        escape = StringUtil.escapeRegexQuoteEncode(escape);
        StringBuilder sb = new StringBuilder(conCommand).append(" -e \"util.exportTable('").append(tableName).append("','").append(filePath).append("',{");
        if(StringUtil.nonEmpty(delimiter)) {
            if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equals(delimiter))
                sb.append("fieldsTerminatedBy:'\\007',");
            else
                sb.append("fieldsTerminatedBy:'").append(delimiter).append("',");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            sb.append("linesTerminatedBy:'").append(lineSeparate).append("',");
        if(StringUtil.nonEmpty(quote))
            sb.append("fieldsOptionallyEnclosed:false,fieldsEnclosedBy:'").append(quote).append("',");
        if(StringUtil.nonEmpty(escape))
            sb.append("fieldsEscapedBy:'").append(escape).append("',");

        if(sb.lastIndexOf(",") == sb.length()-1)
            sb.deleteCharAt(sb.length()-1);
        sb.append("})\"\n");

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
    private String generateLoadSQLByMySQLShell(TableCloneManagerContext tcmContext){
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

        tableName = StringUtil.escapeRegexQuoteEncode(tableName);
        filePath = StringUtil.escapeRegexQuoteEncode(filePath);
        delimiter = StringUtil.escapeRegexQuoteEncode(delimiter);
        lineSeparate = StringUtil.escapeRegexQuoteEncode(lineSeparate);
        quote = StringUtil.escapeRegexQuoteEncode(quote);
        escape = StringUtil.escapeRegexQuoteEncode(escape);


        StringBuilder sb = new StringBuilder(con).append("-e \"util.importTable('").append(filePath).append("', {");
        sb.append("characterSet:'utf8mb4',");
        if(StringUtil.nonEmpty(tableName))
            sb.append("table:'").append(tableName).append("',");
        if(StringUtil.nonEmpty(delimiter)) {
//            sb.append("fieldsTerminatedBy:'").append(delimiter).append("',");
            if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equals(delimiter))
                sb.append("fieldsTerminatedBy:'\\007',");
            else
                sb.append("fieldsTerminatedBy:'").append(delimiter).append("',");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            sb.append("linesTerminatedBy:'").append(lineSeparate).append("',");
        if(StringUtil.nonEmpty(quote))
            sb.append("fieldsEnclosedBy:'").append(quote).append("',");
        if(StringUtil.nonEmpty(escape))
            sb.append("fieldsEscapedBy:'").append(escape).append("',");

        return sb.append("bytesPerChunk:'300M',maxRate:'500M',threads:'8'})\"").toString();
    }

    // ================================================   JDBC   ========================================================
    /**
     * @see <a href="https://stackoverflow.com/questions/53156112/how-to-allow-load-data-local-infile-on-mysql-connector-java">MySQL 8 Load data infile</a>
     */
    private String generateLoadSQLByJDBC(TableCloneManagerContext tcmContext){
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
