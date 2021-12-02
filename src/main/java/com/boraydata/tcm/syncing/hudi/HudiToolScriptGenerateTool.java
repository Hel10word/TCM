package com.boraydata.tcm.syncing.hudi;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.FileUtil;

import java.io.File;

/**
 * @author bufan
 * @data 2021/12/1
 */
public class HudiToolScriptGenerateTool {

    private String initConfigFile(AttachConfig atCfg){
        StringBuilder conf = new StringBuilder();

        conf.append("\nsource.csv=").append(atCfg.getHdfsCsvPath())
            .append("\nsource.jdbc.connect=").append(atCfg.getSourceJdbcConnect())
            .append("\nsource.user=").append(atCfg.getSourceUser())
            .append("\nsource.password=").append(atCfg.getSourcePassword())
            .append("\nsource.database=").append(atCfg.getSourceDatabaseName())
            .append("\nsource.table=").append(atCfg.getSourceTableName())
            .append("\nhive.primary.key=").append(atCfg.getHudiPrimaryKey())
            .append("\nhive.partition.key=").append(atCfg.getHudiPartitionKey())
            .append("\nhive.hdfs.path=").append(atCfg.getHudiHdfsPath())
            .append("\nhive.table=").append(atCfg.getCloneTableName())
            .append("\nhive.table.type=").append(atCfg.getHoodieTableType())
            .append("\nhive.jdbc.connect=").append(atCfg.getCloneJdbcConnect())
            .append("\nhive.user=").append(atCfg.getCloneUser())
            .append("\nhive.password=").append(atCfg.getClonePassword())
            .append("\nhive.database=").append(atCfg.getCloneDatabaseName())
            .append("\nhive.non.partitioned=").append(atCfg.getHiveNonPartitioned())
            .append("\nhive.multi.partition.keys=").append(atCfg.getHiveMultiPartitionKeys())
            .append("\nparallel=").append(atCfg.getParallel())
            .append("\ncdcHome=").append(atCfg.getTempDirectory());

        return conf.toString();
    }

    public boolean initSriptFile(AttachConfig attachConfig){
        String configPath = new File(attachConfig.getTempDirectory(),attachConfig.getSourceDataType() + "_to_HUDI.properties").getPath();
        attachConfig.setLoadExpendScriptPath(configPath);
        return FileUtil.WriteMsgToFile(initConfigFile(attachConfig),configPath);
    }
    public String loadCommand(AttachConfig attachConfig){
        StringBuilder loadShell = new StringBuilder();
        String localCsvPath = attachConfig.getLocalCsvPath();

        if(!FileUtil.Exists(localCsvPath))
            throw new TCMException("unable put CSV file to HDFS,check the CSV file is exists in '"+localCsvPath+'\'');
        loadShell
                .append("hdfs dfs -rm ").append(attachConfig.getHudiHdfsPath())
                .append("\nhdfs dfs -rm ").append(attachConfig.getHdfsCsvPath())
                .append("\nhdfs dfs -put ").append(localCsvPath).append(" ").append(attachConfig.getHdfsCsvDir())
                .append("\njava -cp lib/*:initialization-1.0.jar com.boray.stream.init.InitializationTool ").append(attachConfig.getExportShellPath());

        return loadShell.toString();
    }

}
