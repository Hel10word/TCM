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
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import static com.boraydata.cdc.tcm.utils.StringUtil.escapeJava;

/**
 * Export and Load Table Data by PostgreSQL
 *
 * @author bufan
 * @date 2021/9/26
 */
public class PostgreSQLSyncingTool implements SyncingTool {

    private static final String JDBC_EXPORT_PATH = "STDOUT";
    private static final String JDBC_LOAD_PATH = "STDIN";

    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
//        System.out.println(generateExportSQLByJDBC(tcmContext));
        return generateExportSQLByShell(tcmContext);
//        return generateExportSQLByJDBC(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
//        System.out.println("generateLoadSQLByShell:\n"+generateLoadSQLByShell(tcmContext)+"\n");
        return generateLoadSQLByShell(tcmContext);
//        return generateLoadSQLByJDBC(tcmContext);
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
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;

//        return LoadDataToPostgreSQLByJDBC(tcmContext);
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
    public String generateExportSQLByShell(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        String tableName = "";
        if(Boolean.FALSE.equals(Objects.isNull(tempTable)))
            tableName = "("+tcmContext.getTempTableSelectSQL()+")";
        else
            tableName = sourceTable.getTableName();

        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String connectCommand = getConnectCommand(tcmContext.getSourceConfig());
        String exportSQL = generateExportSQL(tableName,csvPath,delimiter,lineSeparate,quote,escape);
        String exportContent = connectCommand+"\"\\"+escapeJava(exportSQL,"\"")+"\"";
        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }

    private String generateExportSQL(String tableName,String filePath, String delimiter,String lineSeparate,String quote,String escape){
//        tableName = escapeJava(tableName);
        filePath = escapeJava(filePath);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);
        StringBuilder stringBuilder = new StringBuilder("COPY ").append(tableName);
        if(JDBC_EXPORT_PATH.equals(filePath))
            stringBuilder.append(" TO ").append(JDBC_EXPORT_PATH);
        else
            stringBuilder.append(" TO ").append("'").append(escapeJava(filePath,"'")).append("'");

         stringBuilder.append(" WITH ");
        if(StringUtil.nonEmpty(delimiter)) {
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                stringBuilder.append("DELIMITER E'\\007' ");
            else
                stringBuilder.append("DELIMITER '").append(escapeJava(delimiter,"'")).append("' ");
        }
        stringBuilder.append("NULL '' ");
        if(StringUtil.nonEmpty(quote) || StringUtil.nonEmpty(escape))
            stringBuilder.append("CSV ");
        if(StringUtil.nonEmpty(quote))
            stringBuilder.append("QUOTE '").append(escapeJava(quote,"'")).append("' FORCE QUOTE * ");
        if(StringUtil.nonEmpty(escape))
            stringBuilder.append("ESCAPE '").append(escapeJava(escape,"'")).append("' ");
        return stringBuilder.toString();
    }


    /**
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy lineitem from '/usr/local/lineitem.csv' with DELIMITER ',';"

     */
    public String generateLoadSQLByShell(TableCloneManagerContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig());
        String nullStr = "";
//        if(DataSourceEnum.MYSQL.equals(tcmContext.getSourceConfig().getDataSourceEnum()))
//            nullStr = "\\N";

        String loadSQL = generateLoadSQL(tableName,csvPath,delimiter,lineSeparate,quote,escape,nullStr);
        String loadContent = connectCommand+"\"\\"+escapeJava(loadSQL,"\"")+"\"";
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }


    private String generateLoadSQL(String tableName,String filePath, String delimiter,String lineSeparate,String quote,String escape,String nullStr){
        tableName = escapeJava(tableName);
        filePath = escapeJava(filePath);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);
        StringBuilder stringBuilder = new StringBuilder("COPY ").append(tableName);
        if(JDBC_LOAD_PATH.equals(filePath))
            stringBuilder.append(" FROM ").append(JDBC_LOAD_PATH);
        else
            stringBuilder.append(" FROM ").append("'").append(escapeJava(filePath,"'")).append("'");

        stringBuilder.append(" WITH ");
        if(StringUtil.nonEmpty(delimiter)){
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                stringBuilder.append("DELIMITER E'\\007' ");
            else
                stringBuilder.append("DELIMITER '").append(escapeJava(delimiter,"'")).append("' ");
        }
        stringBuilder.append("NULL '' ");
        if(StringUtil.nonEmpty(quote) || StringUtil.nonEmpty(escape))
            stringBuilder.append("CSV ");
        if(StringUtil.nonEmpty(quote))
            stringBuilder.append("QUOTE '").append(escapeJava(quote,"'")).append("' ");
        if(StringUtil.nonEmpty(escape))
            stringBuilder.append("ESCAPE '").append(escapeJava(escape,"'")).append("' ");
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
        String exportSQL = generateExportSQL(tableName,JDBC_EXPORT_PATH,delimiter,lineSeparate,quote,escape);
//        String sql = "COPY " + tableName + " TO STDOUT WITH DELIMITER '"+delimiter+"'";
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
        String loadSQL = generateLoadSQL(tableName,JDBC_LOAD_PATH,delimiter,lineSeparate,quote,escape,nullStr);
//        String sql = "COPY " + tableName + " FROM STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setLoadShellContent(loadSQL);
        return "# PostgreSQL Load Data By JDBC-CopyManager,SQL Statements : "+loadSQL;
    }

    private Boolean ExportDataFromPostgreSQLByJDBC(TableCloneManagerContext tcmContext){
        String filePath = Paths.get(tcmContext.getTempDirectory(),tcmContext.getCsvFileName()).toString();
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
        String filePath = Paths.get(tcmContext.getTempDirectory()+tcmContext.getCsvFileName()).toString();
        String sql = tcmContext.getLoadShellContent();
        File file = FileUtil.getFile(filePath);
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getCloneConfig());
                FileInputStream fileInputStream = new FileInputStream(file)
        ){
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            long l = copyManager.copyIn(sql, fileInputStream,1024*1024);
//            long l = copyManager.copyIn(sql, fileInputStream);
            return true;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
