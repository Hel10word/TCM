package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.mapping.DataMappingSQLTool;
import com.boraydata.tcm.utils.FileUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**  Export and Load Table Data in PgSQL,by shell statements.
 * @author bufan
 * @data 2021/9/26
 */
// =======================================================   Export Load By Shell
//          https://www.postgresql.org/docs/13/sql-copy.html 
// -- export
//psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy (select * from lineitem) to '/usr/local/lineitem.csv' with DELIMITER ',';"
//-- load
//psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy lineitem from '/usr/local/lineitem.csv' with DELIMITER ',';"

public class PgsqlSyncingTool implements SyncingTool {

    @Override
    public String getExportInfo(TableCloneManageContext tcmContext) {
        return generateLoadSQLByShell(tcmContext);
//        return generateExportSQLByJDBC(tcmContext);
    }

    @Override
    public String getLoadInfo(TableCloneManageContext tcmContext) {
//        return generateExportSQLByShell(tcmContext);
        return generateLoadSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeExport(TableCloneManageContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;

//        return ExportDataFromPgSQLByJDBC(tcmContext);
    }

    @Override
    public Boolean executeLoad(TableCloneManageContext tcmContext) {
//        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
//        if(tcmContext.getTcmConfig().getDebug())
//            System.out.println(outStr);
//        return true;

        return LoadDataToPgSQLByJDBC(tcmContext);
    }

    // =================================================================  Shell  =============================================================================

    private String generateExportSQLByShell(TableCloneManageContext tcmContext){
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String loadContent = loadCommand(tcmContext.getCloneConfig(), csvPath,tableName,delimiter);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    private String generateLoadSQLByShell(TableCloneManageContext tcmContext){
        Table table = tcmContext.getFinallySourceTable();
        boolean hudiFlag = DataSourceType.HUDI.equals(tcmContext.getCloneConfig().getDataSourceType());
        String tableName = "";
        // because Hudi Data storage in Hive,Hive Data Type not support Boolean(0,1,"t","f"),just allow ('true','false'),so should mapping this data by select.
        // https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        if(tcmContext.getTempTable() == null)
            tableName = table.getTableName();
        else
            tableName = "("+DataMappingSQLTool.getMappingDataSQL(table,tcmContext.getCloneConfig().getDataSourceType())+")";
        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String exportContent = exportCommand(tcmContext.getSourceConfig(),tableName,csvPath,delimiter,hudiFlag);
        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }

    // psql postgres://postgres:postgres@192.168.30.155:5432/test_db -c "\copy ····"
    // why use \copy ？
    // https://www.postgresql.org/docs/13/app-psql.html#APP-PSQL-META-COMMANDS-COPY
    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s:%s/%s -c  \"?\"",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getPort(),
                config.getDatabasename());
    }

    private String exportCommand(DatabaseConfig config,String tableName, String csvPath,String delimiter,boolean hudiFlag) {
        return completeExportCommand(getConnectCommand(config),tableName,csvPath,delimiter,hudiFlag);
    }

    // Parser CSV File to Hudi by Spark.but Spark parser CSV not standard ,so export CSV File should add " to all flied .
    // https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html
    private String completeExportCommand(String com,String tableName,String filePath, String delimiter,boolean hudiFlag){
        if(hudiFlag)
            return com.replace("?","\\copy "+tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"' CSV QUOTE '\\\"' escape '\\\\' force quote *;");
        else
            return com.replace("?","\\copy "+tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }

    private String loadCommand(DatabaseConfig config, String csvPath,String tablename,String delimiter) {
        return completeLoadCommand(getConnectCommand(config),csvPath,tablename,delimiter);
    }

    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "\\copy "+tableName+" from " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }


    // =========================================  Load And Export By PgSQL-JDBC-CopyManager  ======================================================================

    private String generateExportSQLByJDBC(TableCloneManageContext tcmContext){
        String tableName = tcmContext.getFinallySourceTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String sql = "COPY " + tableName + " TO STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setExportShellContent(sql);
        return "# PgSQL Export Data By JDBC-CopyManager,SQL Statements : "+sql;
    }

    private String generateLoadSQLByJDBC(TableCloneManageContext tcmContext){
        String tableName = tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String sql = "COPY " + tableName + " FROM STDIN WITH DELIMITER '"+delimiter+"'";
        tcmContext.setLoadShellContent(sql);
        return "# PgSQL Load Data By JDBC-CopyManager,SQL Statements : "+sql;
    }

    private Boolean ExportDataFromPgSQLByJDBC(TableCloneManageContext tcmContext){
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

    private Boolean LoadDataToPgSQLByJDBC(TableCloneManageContext tcmContext){
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
