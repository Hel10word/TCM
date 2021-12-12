package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.mapping.DataMappingSQLTool;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;

/**  Export and Load Table Data in PgSQL,by shell statements.
 * @author bufan
 * @data 2021/9/26
 */
public class PgsqlSyncingTool implements SyncingTool {

    @Override
    public String exportFile(TableCloneManageContext tcmContext) {
        Table table = tcmContext.getFinallySourceTable();
        boolean hudiFlag = DataSourceType.HUDI.equals(tcmContext.getCloneConfig().getDataSourceType());
        String exportSQL = "";
        if(tcmContext.getTempTable() == null)
            exportSQL = table.getTablename();
        else
            exportSQL = DataMappingSQLTool.getMappingDataSQL(table,tcmContext.getCloneConfig().getDataSourceType());
        String csvPath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        return exportCommand(tcmContext.getSourceConfig(),exportSQL,csvPath,delimiter,hudiFlag);
    }
    public String exportCommand(DatabaseConfig config,String exportSQL, String csvPath,String delimiter,boolean hudiFlag) {
        return completeExportCommand(getConnectCommand(config),exportSQL,csvPath,delimiter,hudiFlag);
    }

    @Override
    public String loadFile(TableCloneManageContext tcmContext) {
        String csvPath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();
        String tablename = tcmContext.getCloneTable().getTablename();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        return loadCommand(tcmContext.getCloneConfig(), csvPath,tablename,delimiter);
    }
    public String loadCommand(DatabaseConfig config, String csvPath,String tablename,String delimiter) {
        return completeLoadCommand(getConnectCommand(config),csvPath,tablename,delimiter);
    }




    // -- export
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"
    //-- load
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"

    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s/%s -c  \"?\" 2>&1",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getDatabasename());
    }

    private String completeExportCommand(String com,String tableName,String filePath, String delimiter,boolean hudiFlag){
        if(hudiFlag)
            return com.replace("?","\\copy "+tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"' CSV QUOTE '\\\"' escape '\\\\' force quote *;");
        else
            return com.replace("?","\\copy "+tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "\\copy "+tableName+" from " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }





}
