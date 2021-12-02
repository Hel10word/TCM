package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.syncing.hudi.HudiToolScriptGenerateTool;
import com.boraydata.tcm.syncing.hudi.ScalaScriptGenerateTool;
import com.boraydata.tcm.utils.FileUtil;

/**
 * 1. put CSV File into HDFS
 * 2. Generate the *.scala Script
 * 3. spark-shell -i *.scala
 * @author bufan
 * @data 2021/12/1
 */
public class HudiSyncingTool implements SyncingTool {

    @Override
    public boolean exportFile(DatabaseConfig config, AttachConfig attachConfig) {
        return false;
    }

    @Override
    public boolean loadFile(DatabaseConfig config, AttachConfig attachConfig) {


//        HudiToolScriptGenerateTool hudiTool = new HudiToolScriptGenerateTool();
        ScalaScriptGenerateTool hudiTool = new ScalaScriptGenerateTool();

        // Hudi need load HDFS csv File,new generate File Path in HDFS
//        attachConfig.setHdfsCsvPath(new File(attachConfig.getHdfsCsvDir(),attachConfig.getCsvFileName()).getPath().replaceFirst(":/","://"));
        attachConfig.setHdfsCsvPath(attachConfig.getHdfsCsvDir()+attachConfig.getCsvFileName());

        if(!hudiTool.initSriptFile(attachConfig))
            return false;

        String loadCommand = hudiTool.loadCommand(attachConfig);
        String loadShellPath = attachConfig.getLoadShellPath();

//        System.out.println(loadCommand);
//        return true;

        if(FileUtil.WriteMsgToFile(loadCommand,loadShellPath)){
            if(FileUtil.Exists(loadShellPath))
                return CommandExecutor.execuetShell(loadShellPath,attachConfig.getDebug());
        }
        return false;
    }
}
