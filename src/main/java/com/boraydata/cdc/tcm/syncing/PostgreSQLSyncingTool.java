package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Export and Load Table Data by PostgreSQL
 *
 * @author bufan
 * @date 2021/9/26
 */
public class PostgreSQLSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
//        System.out.println(generateExportSQLByJDBC(tcmContext));
        return generateExportSQLByShell(tcmContext);
//        return generateExportSQLByJDBC(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
//        System.out.println("generateLoadSQLByShell:\n"+generateLoadSQLByShell(tcmContext)+"\n");
        return generateLoadSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeExport(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;

//        return ExportDataFromPostgreSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeLoad(TableCloneManagerContext tcmContext) {
//        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
//        if(tcmContext.getTcmConfig().getDebug())
//            System.out.println(outStr);
//        return true;

        return LoadDataToPostgreSQLByJDBC(tcmContext);
    }

    // ================================================   PostgreSQL Syncing Common   ========================================================
    /**
     * @see <a href="https://www.postgresql.org/docs/13/app-psql.html#APP-PSQL-META-COMMANDS-COPY">why use \copy</a>
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c
     */
    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s:%s/%s -c ",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getPort(),
                config.getDatabaseName());
    }

    // =================================================================  Shell  =============================================================================
    /**
     * PostgreSQL official grammar support Export table data and Load table data With SQL Query.
     * @see <a href="https://www.postgresql.org/docs/13/sql-copy.html"></a>
     * @see <a href="https://paquier.xyz/postgresql-2/postgres-9-3-feature-highlight-copy-tofrom-program/"></a>
     */

    /**
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy (select * from lineitem) to '/usr/local/lineitem.csv' with DELIMITER ',';"
     */
    private String generateExportSQLByShell(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        String tableName = "";
        if(Boolean.FALSE.equals(Objects.isNull(tempTable)))
            tableName = "("+tcmContext.getTempTableSelectSQL()+")";
        else
            tableName = sourceTable.getTableName();

        String csvPath = "'./"+tcmContext.getCsvFileName()+"'";
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String connectCommand = getConnectCommand(tcmContext.getSourceConfig());
        String exportSQL = StringUtil.escapeRegexDoubleQuoteEncode(generateExportSQL(tableName,csvPath,delimiter,lineSeparate,quote,escape).replaceAll("\\\\","\\\\\\\\"));
        String exportContent = connectCommand+"\"\\"+exportSQL+"\"";
        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }

    private String generateExportSQL(String tableName,String filePath, String delimiter,String lineSeparate,String quote,String escape){
//        tableName = StringUtil.escapeRegexSingleQuoteEncode(tableName);
//        filePath = StringUtil.escapeRegexSingleQuoteEncode(filePath);
        delimiter = StringUtil.escapeRegexSingleQuoteEncode(delimiter);
        lineSeparate = StringUtil.escapeRegexSingleQuoteEncode(lineSeparate);
        quote = StringUtil.escapeRegexSingleQuoteEncode(quote);
        escape = StringUtil.escapeRegexSingleQuoteEncode(escape);
        StringBuilder stringBuilder = new StringBuilder("COPY ").append(tableName)
                .append(" TO ").append(filePath)
                .append(" WITH ");
        if(StringUtil.nonEmpty(delimiter)) {
            if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equals(delimiter))
                stringBuilder.append("DELIMITER E'\\007' ");
            else
                stringBuilder.append("DELIMITER '").append(delimiter).append("' ");
        }
        stringBuilder.append("NULL '' ");
        if(StringUtil.nonEmpty(quote) || StringUtil.nonEmpty(escape))
            stringBuilder.append("CSV ");
        if(StringUtil.nonEmpty(quote))
            stringBuilder.append("QUOTE '").append(quote).append("' FORCE QUOTE * ");
        if(StringUtil.nonEmpty(escape))
            stringBuilder.append("ESCAPE '").append(escape).append("' ");
        return stringBuilder.toString();
    }


    /**
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy lineitem from '/usr/local/lineitem.csv' with DELIMITER ',';"

     */
    private String generateLoadSQLByShell(TableCloneManagerContext tcmContext){
        String csvPath = "'./"+tcmContext.getCsvFileName()+"'";
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig());
        String nullStr = "";
//        if(DataSourceEnum.MYSQL.equals(tcmContext.getSourceConfig().getDataSourceEnum()))
//            nullStr = "\\N";

        String loadSQL = StringUtil.escapeRegexDoubleQuoteEncode(generateLoadSQL(tableName,csvPath,delimiter,lineSeparate,quote,escape,nullStr).replaceAll("\\\\","\\\\\\\\"));
        String loadContent = connectCommand+"\"\\"+loadSQL+"\"";
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }
    private String generateLoadSQL(String tableName,String filePath, String delimiter,String lineSeparate,String quote,String escape,String nullStr){
        tableName = StringUtil.escapeRegexSingleQuoteEncode(tableName);
//        filePath = StringUtil.escapeRegexSingleQuoteEncode(filePath);
        delimiter = StringUtil.escapeRegexSingleQuoteEncode(delimiter);
        lineSeparate = StringUtil.escapeRegexSingleQuoteEncode(lineSeparate);
        quote = StringUtil.escapeRegexSingleQuoteEncode(quote);
        escape = StringUtil.escapeRegexSingleQuoteEncode(escape);
        StringBuilder stringBuilder = new StringBuilder("COPY ").append(tableName)
                .append(" FROM ").append(filePath)
                .append(" WITH ");
        if(StringUtil.nonEmpty(delimiter)){
            if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equals(delimiter))
                stringBuilder.append("DELIMITER E'\\007' ");
            else
                stringBuilder.append("DELIMITER '").append(delimiter).append("' ");
        }
        stringBuilder.append("NULL '' ");
        if(StringUtil.nonEmpty(quote) || StringUtil.nonEmpty(escape))
            stringBuilder.append("CSV ");
        if(StringUtil.nonEmpty(quote))
            stringBuilder.append("QUOTE '").append(quote).append("' ");
        if(StringUtil.nonEmpty(escape))
            stringBuilder.append("ESCAPE '").append(escape).append("' ");
        return stringBuilder.toString();
    }



    // =========================================  Load And Export By PostgreSQL-JDBC-CopyManager  ======================================================================

    /**
     * PostgreSQL also can use JDBC to Export or Load table data,the performance better than shell.
     * @see <a href="https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html"></a>
     */
    private String generateExportSQLByJDBC(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        String tableName = "";
        if(Boolean.TRUE.equals(Objects.isNull(tempTable)))
            tableName = "("+tcmContext.getTempTableSelectSQL()+")";
        else
            tableName = sourceTable.getTableName();

        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String exportSQL = generateExportSQL(tableName,"STDIN",delimiter,lineSeparate,quote,escape);
//        String sql = "COPY " + tableName + " TO STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setExportShellContent(exportSQL);
        return "# PostgreSQL Export Data By JDBC-CopyManager,SQL Statements : "+exportSQL;
    }

    private String generateLoadSQLByJDBC(TableCloneManagerContext tcmContext){
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String nullStr = "";
//        if(DataSourceEnum.MYSQL.equals(tcmContext.getSourceConfig().getDataSourceEnum()))
//            nullStr = "\\N";
        String loadSQL = generateLoadSQL(tableName,"STDIN",delimiter,lineSeparate,quote,escape,nullStr);
//        String sql = "COPY " + tableName + " FROM STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setLoadShellContent(loadSQL);
        return "# PostgreSQL Load Data By JDBC-CopyManager,SQL Statements : "+loadSQL;
    }

    private Boolean ExportDataFromPostgreSQLByJDBC(TableCloneManagerContext tcmContext){
        String filePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String sql = tcmContext.getExportShellContent();
        File file = FileUtil.createNewFile(filePath);
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getSourceConfig());
                FileOutputStream fileOutputStream = new FileOutputStream(file)
        ){
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            long l = copyManager.copyOut(sql, fileOutputStream);
            return true;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private Boolean LoadDataToPostgreSQLByJDBC(TableCloneManagerContext tcmContext){
        String filePath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String sql = tcmContext.getLoadShellContent();
        File file = FileUtil.getFile(filePath);
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getCloneConfig());
                FileInputStream fileInputStream = new FileInputStream(file)
        ){
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            long l = copyManager.copyIn(sql, fileInputStream);
            return true;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
