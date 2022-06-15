package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.FileUtil;
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
 * Export and Load Table Data by PgSQL
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
 * @date 2021/9/26
 */
public class PgsqlSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
        return generateExportSQLByShell(tcmContext);
//        return generateExportSQLByJDBC(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
        return generateLoadSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeExport(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;

//        return ExportDataFromPgSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeLoad(TableCloneManagerContext tcmContext) {
//        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
//        if(tcmContext.getTcmConfig().getDebug())
//            System.out.println(outStr);
//        return true;

        return LoadDataToPgSQLByJDBC(tcmContext);
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/13/app-psql.html#APP-PSQL-META-COMMANDS-COPY">why use \copy</a>
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy ?"
     */
    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s:%s/%s -c  \"\\copy ?\"",
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
     */

    /**
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy lineitem from '/usr/local/lineitem.csv' with DELIMITER ',';"
     */
    private String generateExportSQLByShell(TableCloneManagerContext tcmContext){
        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        boolean hudiFlag = DataSourceEnum.HUDI.equals(tcmContext.getCloneConfig().getDataSourceEnum());

        /**
         *  because Hudi Data storage in Hive,Hive Data Type not support Boolean(0,1,"t","f"),
         *  just allow ('true','false'),so should mapping this data by select.
         * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"></a>
         */
        String tableName = "";
        if(Boolean.FALSE.equals(Objects.isNull(tempTable)))
            tableName = "("+tcmContext.getTempTableSelectSQL()+")";
        else
            tableName = sourceTable.getTableName();

        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String connectCommand = getConnectCommand(tcmContext.getSourceConfig());
        String exportContent = replaceExportStatementShell(connectCommand,tableName,csvPath,delimiter,hudiFlag);
        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }

    private String replaceExportStatementShell(String com,String tableName,String filePath, String delimiter,boolean hudiFlag){
        if(hudiFlag)
            return com.replace("?",tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"' CSV QUOTE '\\\"' escape '\\\\' force quote *;");
        else
            return com.replace("?",tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }


    /**
     * @author: bufan
     * @return: psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy (select * from lineitem) to '/usr/local/lineitem.csv' with DELIMITER ',';"
     */
    private String generateLoadSQLByShell(TableCloneManagerContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String connectCommand = getConnectCommand(tcmContext.getCloneConfig());
        String loadContent = replaceLoadStatementShell(connectCommand, csvPath,tableName,delimiter);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }
    private String replaceLoadStatementShell(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                tableName+" from " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }



    // =========================================  Load And Export By PgSQL-JDBC-CopyManager  ======================================================================

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
        String sql = "COPY " + tableName + " TO STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setExportShellContent(sql);
        return "# PgSQL Export Data By JDBC-CopyManager,SQL Statements : "+sql;
    }

    private String generateLoadSQLByJDBC(TableCloneManagerContext tcmContext){
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String sql = "COPY " + tableName + " FROM STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setLoadShellContent(sql);
        return "# PgSQL Load Data By JDBC-CopyManager,SQL Statements : "+sql;
    }

    private Boolean ExportDataFromPgSQLByJDBC(TableCloneManagerContext tcmContext){
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

    private Boolean LoadDataToPgSQLByJDBC(TableCloneManagerContext tcmContext){
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
