//package com.boraydata.tcm.syncing.hudi;
//
//import com.boraydata.tcm.configuration.TableCloneManageConfig;
//import com.boraydata.tcm.exception.TCMException;
//import com.boraydata.tcm.utils.FileUtil;
//
//import java.io.File;
//
///**
// * @author bufan
// * @data 2021/12/1
// */
//public class HudiToolScriptGenerateTool {
//
//    private String initConfigFile(TableCloneManageConfig atCfg){
//        StringBuilder conf = new StringBuilder();
//
//        conf.append("\nsource.csv=").append(atCfg.getHdfsCsvPath())
//            .append("\nsource.jdbc.connect=").append(atCfg.getSourceJdbcConnect())
//            .append("\nsource.user=").append(atCfg.getSourceUser())
//            .append("\nsource.password=").append(atCfg.getSourcePassword())
//            .append("\nsource.database=").append(atCfg.getSourceDatabaseName())
//            .append("\nsource.table=").append(atCfg.getSourceTableName())
//            .append("\nhive.primary.key=").append(atCfg.getPrimaryKey())
//            .append("\nhive.partition.key=").append(atCfg.getPartitionKey())
//            .append("\nhive.hdfs.path=").append(atCfg.getHdfsCloneDataPath())
//            .append("\nhive.table=").append(atCfg.getCloneTableName())
//            .append("\nhive.table.type=").append(atCfg.getHoodieTableType())
//            .append("\nhive.jdbc.connect=").append(atCfg.getCloneJdbcConnect())
//            .append("\nhive.user=").append(atCfg.getCloneUser())
//            .append("\nhive.password=").append(atCfg.getClonePassword())
//            .append("\nhive.database=").append(atCfg.getCloneDatabaseName())
//            .append("\nhive.non.partitioned=").append(atCfg.getHiveNonPartitioned())
//            .append("\nhive.multi.partition.keys=").append(atCfg.getHiveMultiPartitionKeys())
//            .append("\nparallel=").append(atCfg.getParallel())
//            .append("\ncdcHome=").append(atCfg.getTempDirectory());
//
//        return conf.toString();
//    }
//
//    public boolean initSriptFile(TableCloneManageConfig attachConfig){
//        String configPath = new File(attachConfig.getTempDirectory(),attachConfig.getSourceDataType() + "_to_HUDI.properties").getPath();
//        attachConfig.setLoadDataScript(configPath);
//        return FileUtil.WriteMsgToFile(initConfigFile(attachConfig),configPath);
//    }
//    public String loadCommand(TableCloneManageConfig attachConfig){
//        StringBuilder loadShell = new StringBuilder();
//        String localCsvPath = attachConfig.getLocalCsvPath();
//
//        if(!FileUtil.Exists(localCsvPath))
//            throw new TCMException("unable put CSV file to HDFS,check the CSV file is exists in '"+localCsvPath+'\'');
//        loadShell
//                .append("hdfs dfs -rm ").append(attachConfig.getHdfsCloneDataPath())
//                .append("\nhdfs dfs -rm ").append(attachConfig.getHdfsCsvPath())
//                .append("\nhdfs dfs -put ").append(localCsvPath).append(" ").append(attachConfig.getHdfsSourceDataDir())
//                .append("\njava -cp lib/*:initialization-1.0.jar com.boray.stream.init.InitializationTool ").append(attachConfig.getExportShellName());
//
//        return loadShell.toString();
//    }
//
//}
