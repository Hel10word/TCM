package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.FileUtil;

import java.io.File;
import java.io.IOException;

/**
 * @author bufan
 * @data 2021/11/4
 */
public class HudiSyncingTool implements SyncingTool {


    @Override
    public boolean exportFile(DatabaseConfig config, AttachConfig attachConfig) {
        return false;
    }

    @Override
    public boolean loadFile(DatabaseConfig config, AttachConfig atCfg) {
        return putToHDFS(atCfg);
    }

    private boolean putToHDFS(AttachConfig attachConfig){
        String command = "";
        String localCsvPath = attachConfig.getLocalCsvPath();
//        attachConfig.setHdfsCsvPath(new File(attachConfig.getHdfsCsvDir(),attachConfig.getCsvFileName()).getPath().replaceFirst(":/","://"));
        attachConfig.setHdfsCsvPath(attachConfig.getHdfsCsvDir()+attachConfig.getCsvFileName());
        if(!FileUtil.Exists(localCsvPath))
                throw new TCMException("unable put CSV file to HDFS,check the CSV file is exists in '"+localCsvPath+'\'');
        command += "\nhdfs dfs -rm "+attachConfig.getHiveHdfsPath();
        command += "\nhdfs dfs -rm "+attachConfig.getHdfsCsvPath();
        command += "\nmkdir -p "+attachConfig.getTempDirectory()+"/init/";
//        command += "\nrm -f ./*.avsc";
        command += "\nhdfs dfs -put "+localCsvPath+" "+attachConfig.getHdfsCsvDir();



        String s = new File(attachConfig.getTempDirectory(),attachConfig.getSourceDataType() + "_to_HUDI.properties").getPath();
        if(callDB2Hive(attachConfig,s)){
            command += "\njava -cp lib/*:initialization-1.0.jar com.boray.stream.init.InitializationTool "+s;
        }

        String loadShellPath = attachConfig.getLoadShellPath();
//        return FileUtil.WriteMsgToFile(command,loadShellPath);
        if(FileUtil.WriteMsgToFile(command,loadShellPath)){
            return CommandExecutor.execuetShell(loadShellPath,attachConfig.getDebug());
        }
        return false;
    }

    private boolean callDB2Hive(AttachConfig atCfg,String configPath){

        String sourceJdbcConnect = atCfg.getSourceJdbcConnect();
        String sourceUser = atCfg.getSourceUser();
        String sourcePassword = atCfg.getSourcePassword();
        String sourceDatabaseName = atCfg.getSourceDatabaseName();

        String cloneJdbcConnect = atCfg.getCloneJdbcConnect();
        String cloneUser = atCfg.getCloneUser();
        String clonePassword = atCfg.getClonePassword();
        String cloneDatabaseName = atCfg.getCloneDatabaseName();

        String hdfsCsvPath = atCfg.getHdfsCsvPath();
        String sourceTableName = atCfg.getSourceTableName();
        String hivePrimaryKey = atCfg.getHivePrimaryKey();
        String hivePartitionKey = atCfg.getHivePartitionKey();
        String hiveHdfsPath = atCfg.getHiveHdfsPath();
        String cloneTableName = atCfg.getCloneTableName();
        String hoodieTableType = atCfg.getHoodieTableType();
        Boolean hiveNonPartitioned = atCfg.getHiveNonPartitioned();
        Boolean hiveMultiPartitionKeys = atCfg.getHiveMultiPartitionKeys();
        String parallel = atCfg.getParallel();
//        String cdcHome = props.getProperty("cdc.home", null);
        String cdcHome = atCfg.getTempDirectory();
//        String tempDirectory = atCfg.getTempDirectory();

        if (hivePrimaryKey == null || hivePrimaryKey.length() < 1) {
            System.out.println("Parameter error. `hive.primary.key` is empty");
            return false;
        }

        return writeProperties(
                configPath,sourceJdbcConnect, sourceUser, sourcePassword, sourceDatabaseName, sourceTableName,
                parallel, hivePrimaryKey,hivePartitionKey, hdfsCsvPath, hiveHdfsPath, cloneTableName, hoodieTableType,
                cloneJdbcConnect, cloneUser, clonePassword, cloneDatabaseName,
                hiveNonPartitioned, hiveMultiPartitionKeys, cdcHome);

//        Parameter parameter = new Parameter(
//                sourceJdbcConnect, sourceUser, sourcePassword, sourceDatabaseName, sourceTableName,
//                parallel, hivePrimaryKey,hivePartitionKey, hdfsCsvPath, hiveHdfsPath, cloneTableName, hoodieTableType,
//                cloneJdbcConnect, cloneUser, clonePassword, cloneDatabaseName,
//                hiveNonPartitioned, hiveMultiPartitionKeys, null);
//
//        HudiBulkInsert bulkInsert = new HudiBulkInsert(parameter);
//        long t1 = System.currentTimeMillis();
//        // read source2
//        bulkInsert.avroGenerator();
//        bulkInsert.configGenerator();
//
//        long t2 = System.currentTimeMillis();
//
//        // write sink
//        bulkInsert.insertHDFS();
//
//        long t3 = System.currentTimeMillis();
//
//        // sync hive
//        bulkInsert.syncHive();
//        long t4 = System.currentTimeMillis();
//
//        System.out.println("read schema time : " + (t2 - t1)/1000);
//        System.out.println("write hdfs  time : " + (t3 - t2)/1000);
//        System.out.println("sync hive   time : " + (t4 - t3)/1000);
//        return false;
    }

    private boolean writeProperties(String configPath,String pgConnUrl, String pgUser, String pgPassword,
                                    String pgDatabase, String pgTable, String parallel,
                                    String primaryKey, String partitionKey, String csvPath,
                                    String hdfsPath, String hiveTable, String hoodieTableType,
                                    String hiveUrl, String hiveUser, String hivePassword,
                                    String hiveDatabase, Boolean nonPartitioned, Boolean hiveMultiPartitionKeys,
                                    String cdcHome){
        String config = "";
        config+="\nsource.csv="+csvPath;
        config+="\nsource.jdbc.connect="+pgConnUrl;
        config+="\nsource.user="+pgUser;
        config+="\nsource.password="+pgPassword;
        config+="\nsource.database="+pgDatabase;
        config+="\nsource.table="+pgTable;
        config+="\nhive.primary.key="+primaryKey;
        config+="\nhive.partition.key="+partitionKey;
        config+="\nhive.hdfs.path="+hdfsPath;
        config+="\nhive.table="+hiveTable;
        config+="\nhive.table.type="+hoodieTableType;
        config+="\nhive.jdbc.connect="+hiveUrl;
        config+="\nhive.user="+hiveUser;
        config+="\nhive.password="+hivePassword;
        config+="\nhive.database="+hiveDatabase;
        config+="\nhive.non.partitioned="+nonPartitioned;
        config+="\nhive.multi.partition.keys="+hiveMultiPartitionKeys;
        config+="\nparallel="+parallel;
        config+="\ncdcHome="+cdcHome;

        return FileUtil.WriteMsgToFile(config,configPath);
    }




}
