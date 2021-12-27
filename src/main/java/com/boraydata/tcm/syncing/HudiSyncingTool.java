package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.syncing.hudi.ScalaScriptGenerateTool;
import com.boraydata.tcm.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public String exportFile(TableCloneManageContext tcmContext) {
        return null;
    }

    @Override
    public String loadFile(TableCloneManageContext tcmContext) {
        TableCloneManageConfig tcmConfig = tcmContext.getTcmConfig();
        DatabaseConfig cloneConfig = tcmContext.getCloneConfig();
        ScalaScriptGenerateTool hudiTool = new ScalaScriptGenerateTool();

        Table cloneTable = tcmContext.getCloneTable();
        String hdfsSourceDataDir = tcmConfig.getHdfsSourceDataDir();
        String hdfsSourceFileName = hdfsSourceDataDir + tcmContext.getCsvFileName();
        String scriptContent = hudiTool.initSriptFile(cloneTable,hdfsSourceFileName,cloneConfig,tcmConfig);

        String scriptName = "Load_CSV_to_hudi_"+tcmConfig.getHoodieTableType()+".scala";
        String scriptPath = tcmContext.getTempDirectory()+scriptName;
        String localCsvPath = tcmContext.getTempDirectory()+tcmContext.getCsvFileName();

        tcmContext.setLoadDataScriptContent(scriptContent);

        FileUtil.WriteMsgToFile(scriptContent,scriptPath);
        tcmContext.setLoadDataScriptName(scriptName);

        return hudiTool.loadCommand(hdfsSourceDataDir,localCsvPath,tcmConfig.getHdfsCloneDataPath(),scriptPath,tcmConfig.getSparkCustomCommand());
    }

}
