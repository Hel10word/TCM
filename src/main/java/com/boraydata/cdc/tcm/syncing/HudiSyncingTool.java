package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.TableCloneManageConfig;
import com.boraydata.cdc.tcm.core.TableCloneManageContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.syncing.util.ScalaScriptGenerateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 1. put CSV File into HDFS
 * 2. Generate the *.scala Script
 * 3. spark-shell -i *.scala
 * @author bufan
 * @data 2021/12/1
 */
public class HudiSyncingTool implements SyncingTool {

    private static final Logger logger = LoggerFactory.getLogger(HudiSyncingTool.class);

    @Override
    public String getExportInfo(TableCloneManageContext tcmContext) {
        tcmContext.setExportShellContent("TableCloneManage Un Suppose Export from Hudi");
        return null;
    }

    @Override
    public String getLoadInfo(TableCloneManageContext tcmContext) {
        TableCloneManageConfig tcmConfig = tcmContext.getTcmConfig();
        DatabaseConfig cloneConfig = tcmContext.getCloneConfig();
        ScalaScriptGenerateUtil hudiTool = new ScalaScriptGenerateUtil();

        Table cloneTable = tcmContext.getCloneTable();
        String hdfsSourceDataDir = tcmConfig.getHdfsSourceDataDir();
        String hdfsSourceFileName = hdfsSourceDataDir + tcmContext.getCsvFileName();
        String scriptContent = hudiTool.initSriptFile(cloneTable,hdfsSourceFileName,cloneConfig,tcmConfig);

        String scriptName = "Load_CSV_to_hudi_"+tcmConfig.getHoodieTableType()+".scala";
        String scriptPath = "./"+scriptName;
        String localCsvPath = "./"+tcmContext.getCsvFileName();

        tcmContext.setLoadDataInHudiScalaScriptContent(scriptContent);
        tcmContext.setLoadDataInHudiScalaScriptName(scriptName);

        String loadContent = hudiTool.loadCommand(hdfsSourceDataDir,localCsvPath,tcmConfig.getHdfsCloneDataPath(),scriptPath,tcmConfig.getSparkCustomCommand());
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    @Override
    public Boolean executeExport(TableCloneManageContext tcmContext) {
        return null;
    }

    @Override
    public Boolean executeLoad(TableCloneManageContext tcmContext) {
        deleteOriginTable(tcmContext);
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            logger.info(outStr);
        return true;
    }

    public Boolean deleteOriginTable(TableCloneManageContext tcmContext){
        String tableName = tcmContext.getTcmConfig().getCloneTableName();
        String hoodieTableType = tcmContext.getTcmConfig().getHoodieTableType();
        try(
            Connection conn = DatasourceConnectionFactory.createDataSourceConnection(tcmContext.getCloneConfig());
            Statement statement = conn.createStatement()
        ){
//            boolean autoCommit = conn.getAutoCommit();
//            conn.setAutoCommit(false);
//            ("COPY_ON_WRITE".equalsIgnoreCase(hoodieTableType))
            if("MERGE_ON_READ".equalsIgnoreCase(hoodieTableType)){
//                statement.addBatch("drop table if exists "+tableName+"_ro;");
//                statement.addBatch("drop table if exists "+tableName+"_rt;");
                statement.execute("drop table if exists "+tableName+"_ro");
                statement.execute("drop table if exists "+tableName+"_rt");
            }else{
//                statement.addBatch("drop table if exists "+tableName);
                statement.execute("drop table if exists "+tableName);
            }
//            statement.executeBatch();
//            conn.commit();
//            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw new TCMException("deleteOriginTable Failed tableName:'"+tableName+"' hoodieTableType:'"+hoodieTableType+"'",e);
        }
        return true;
    }

}
