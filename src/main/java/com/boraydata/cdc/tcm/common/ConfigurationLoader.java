package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.JacksonUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author bufan
 * @data 2022/5/16
 */
public class ConfigurationLoader {

    public static TableCloneManageConfig loadConfigFile(String path) throws IOException {
        String fileExtension = FileUtil.getFileExtension(path);
        if(fileExtension.equals("properties")) {
            FileInputStream in = new FileInputStream(path);
            Properties properties = new Properties();
            properties.load(in);
            TableCloneManageConfig tableCloneManageConfig = loadPropertiesConfig(properties);
            in.close();
            return tableCloneManageConfig.checkConfig();
        }else if(fileExtension.equals("json")){
            TableCloneManageConfig tableCloneManageConfig = JacksonUtil.filePathToObject(path,TableCloneManageConfig.class);
            return tableCloneManageConfig.checkConfig();
        }else
            throw new TCMException("unable support config file type,file path:"+path);
    }

    private static TableCloneManageConfig loadPropertiesConfig(Properties props){
        TableCloneManageConfig tcmConfig = new TableCloneManageConfig();

        //======================================================    Source DatabaseConfig   ==================================================
        DatabaseConfig sourceConfig = new DatabaseConfig()
                .setDataSourceEnum(DataSourceEnum.valueOfByString(props.getProperty("sourceDataType")))
                .setHost(props.getProperty("sourceHost"))
                .setPort(props.getProperty("sourcePort"))
                .setUsername(props.getProperty("sourceUser"))
                .setPassword(props.getProperty("sourcePassword"))
                .setDatabaseName(props.getProperty("sourceDatabaseName"))
                .setCatalog(props.getProperty("sourceCatalog"))
                .setSchema(props.getProperty("sourceSchema"))
                .setDriver(props.getProperty("sourceDriver"))
                .setJdbcUrl(props.getProperty("sourceJdbcUrl"));

        //======================================================    Clone DatabaseConfig   ==================================================
        DatabaseConfig cloneConfig = new DatabaseConfig()
                .setDataSourceEnum(DataSourceEnum.valueOfByString(props.getProperty("cloneDataType")))
                .setHost(props.getProperty("cloneHost"))
                .setPort(props.getProperty("clonePort"))
                .setUsername(props.getProperty("cloneUser"))
                .setPassword(props.getProperty("clonePassword"))
                .setDatabaseName(props.getProperty("cloneDatabaseName"))
                .setCatalog(props.getProperty("cloneCatalog"))
                .setSchema(props.getProperty("cloneSchema"))
                .setDriver(props.getProperty("cloneDriver"))
                .setJdbcUrl(props.getProperty("cloneJdbcUrl"));

        tcmConfig
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .setSourceTableName(props.getProperty("sourceTableName"))
                .setCloneTableName(props.getProperty("cloneTableName",tcmConfig.getSourceTableName()))
                // custom schema structure
                .setCustomSchemaFilePath(props.getProperty("customSchemaFilePath",""));



        //======================================================    Hudi Config   ==================================================

        /**
         * put CSV File in HDFS Path
         * @see <a href="https://hudi.apache.org/docs/configurations"></a>
         */
        tcmConfig.setHdfsSourceDataDir(props.getProperty("hdfsSourceDataPath"))
                // Hudi Data Save Path
                .setHdfsCloneDataPath(props.getProperty("hdfsCloneDataPath"))
                .setPrimaryKey(props.getProperty("primaryKey"))
                .setPartitionKey(props.getProperty("partitionKey","_hoodie_date"))
                // The table type for the underlying data, for this write.   e.g MERGE_ON_READ、COPY_ON_WRITE
                .setHoodieTableType(props.getProperty("hudiTableType","MERGE_ON_READ"))
                .setNonPartition(Boolean.parseBoolean(props.getProperty("nonPartition", "false").toLowerCase()))
                .setMultiPartitionKeys(Boolean.parseBoolean(props.getProperty("multiPartitionKeys", "false").toLowerCase()));
//        String hiveNonPartitioned = Boolean.parseBoolean(props.getProperty("hive.non.partitioned", "false").toLowerCase());
//        String hiveMultiPartitionKeys = Boolean.parseBoolean(props.getProperty("hive.multi.partition.keys", "false").toLowerCase());

        //======================================================    Spark Config   ==================================================

        tcmConfig.setSparkCustomCommand(props.getProperty("sparkCustomCommand"));

        //======================================================    TCM Tools Config   ==================================================
                // get source table sql statement
        tcmConfig.setOutSourceTableSQL(Boolean.parseBoolean(props.getProperty("outSourceTableSQL","false").toLowerCase()))
                // get clone table sql statement
                .setOutCloneTableSQL(Boolean.parseBoolean(props.getProperty("outCloneTableSQL","false").toLowerCase()))
                // create clone table in clone database
                .setCreateTableInClone(Boolean.parseBoolean(props.getProperty("createTableInClone","true").toLowerCase()))
                // execute export csv shell script
                .setExecuteExportScript(Boolean.parseBoolean(props.getProperty("executeExportScript","true").toLowerCase()))
                // execute import csv shell script
                .setExecuteLoadScript(Boolean.parseBoolean(props.getProperty("executeLoadScript","true").toLowerCase()));

        // use column store in memsql
//        this.memsqlColumnStore = props.getProperty("memsqlColumnStore","");

        // export csv file name,default:  "Export_from_"+SourceDatabases.Type+"_"+SourceTable.Name+".csv"  => Export_from_POSTGRES_lineitem.csv
        tcmConfig.setCsvFileName(props.getProperty("csvFileName",""))
                // save temp files directory
                .setDeleteCache(Boolean.parseBoolean(props.getProperty("deleteCache","true").toLowerCase()))
                // save temp files directory
                .setTempDirectory(props.getProperty("tempDirectory","./TCM-TempData/"))
                // delimiter field in csv
                .setDelimiter(props.getProperty("delimiter","|"))
                // Control the output of process information
                .setDebug(Boolean.parseBoolean(props.getProperty("debug","false").toLowerCase()));

        return tcmConfig;
    }

}
