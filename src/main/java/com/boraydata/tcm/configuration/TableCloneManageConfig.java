package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

import java.util.Properties;

/** Load configuration file information
 * @author bufan
 * @data 2021/11/4
 */
public class TableCloneManageConfig {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;

    private String hdfsSourceDataDir;
    private String hdfsCloneDataPath;
    private String primaryKey;
    private String partitionKey;
    private String hoodieTableType;

    private String sparkCustomCommand;

    private Boolean getSourceTableSQL;
    private Boolean getCloneTableSQL;
    private Boolean createTableInClone;
    private Boolean executeExportScript;
    private Boolean executeImportScript;
    private String csvFileName;
    private String memsqlColumnStore;
    private Boolean deleteCache;
    private String tempDirectory;
    private String delimiter;
    private Boolean debug;

    private TableCloneManageConfig() {
        //
    }
    private static TableCloneManageConfig instance = new TableCloneManageConfig();

    public static TableCloneManageConfig getInstance(){
        return instance;
    }

    public TableCloneManageConfig loadLocalConfig(Properties props){
        //======================================================    Source DatabaseConfig   ==================================================
        String sourceDatabaseName = props.getProperty("sourceDatabaseName");
        String sourceDataType = props.getProperty("sourceDataType");
        String sourceHost = props.getProperty("sourceHost");
        String sourcePort = props.getProperty("sourcePort");
        String sourceUser = props.getProperty("sourceUser");
        String sourcePassword = props.getProperty("sourcePassword");
        String sourceTableName = props.getProperty("sourceTable");

        //======================================================    Clone DatabaseConfig   ==================================================
        String cloneDatabaseName = props.getProperty("cloneDatabaseName");
        String cloneDataType = props.getProperty("cloneDataType");
        String cloneHost = props.getProperty("cloneHost");
        String clonePort = props.getProperty("clonePort");
        String cloneUser = props.getProperty("cloneUser");
        String clonePassword = props.getProperty("clonePassword");
        String cloneTableName = props.getProperty("cloneTable",sourceTableName);

        //======================================================    Hudi Config   ==================================================
        // https://hudi.apache.org/docs/configurations
        // put CSV File in HDFS Path
        this.hdfsSourceDataDir = props.getProperty("hdfs.source.data.path");
        // Hudi Data Save Path
        this.hdfsCloneDataPath = props.getProperty("hdfs.clone.data.path");
        this.primaryKey = props.getProperty("primary.key");
        this.partitionKey = props.getProperty("partition.key","_hoodie_date");
        // The table type for the underlying data, for this write.   e.g MERGE_ON_READ、COPY_ON_WRITE
        this.hoodieTableType = props.getProperty("hudi.table.type","MERGE_ON_READ");
//        this.hiveNonPartitioned = Boolean.parseBoolean(props.getProperty("hive.non.partitioned", "false").toLowerCase());
//        this.hiveMultiPartitionKeys = Boolean.parseBoolean(props.getProperty("hive.multi.partition.keys", "false").toLowerCase());

        //======================================================    Spark Config   ==================================================

        this.sparkCustomCommand = props.getProperty("spark.custom.command");

        //======================================================    TCM Tools Config   ==================================================
        // get source table sql statement
        this.getSourceTableSQL = Boolean.parseBoolean(props.getProperty("getSourceTableSQL","false").toLowerCase());
        // get clone table sql statement
        this.getCloneTableSQL = Boolean.parseBoolean(props.getProperty("getCloneTableSQL","false").toLowerCase());
        // create clone table in clone database
        this.createTableInClone = Boolean.parseBoolean(props.getProperty("createTableInClone","true").toLowerCase());
        // execute export csv shell script
        this.executeExportScript = Boolean.parseBoolean(props.getProperty("executeExportScript","true").toLowerCase());
        // execute import csv shell script
        this.executeImportScript = Boolean.parseBoolean(props.getProperty("executeImportScript","true").toLowerCase());

        // use column store in memsql
        this.memsqlColumnStore = props.getProperty("memsqlColumnStore","");

        // export csv file name,default:  "Export_from_"+SourceDatabases.Type+"_"+SourceTable.Name+".csv"  => Export_from_POSTGRES_lineitem.csv
        this.csvFileName = props.getProperty("csvFileName","");


        // save temp files directory
        this.deleteCache = Boolean.parseBoolean(props.getProperty("deleteCache","true").toLowerCase());
        // save temp files directory
        this.tempDirectory = props.getProperty("tempDirectory","./TCM-TempData/");
        // delimiter field in csv
        this.delimiter = props.getProperty("delimiter","|");
        // Control the output of process information
        this.debug = Boolean.parseBoolean(props.getProperty("debug","false").toLowerCase());

        //======================================================     check config

        if (sourceDataType.equalsIgnoreCase("hudi")) {
            throw new TCMException("sourceDataType unable to be "+sourceDataType);
        }

        DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder()
                .setDatabasename(sourceDatabaseName)
                .setDataSourceType(DataSourceType.getTypeByStr(sourceDataType))
                .setHost(sourceHost)
                .setPort(sourcePort)
                .setUsername(sourceUser)
                .setPassword(sourcePassword);
        this.sourceConfig = sourceBuilder.create();

        DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder()
                .setDatabasename(cloneDatabaseName)
                .setDataSourceType(DataSourceType.getTypeByStr(cloneDataType))
                .setHost(cloneHost)
                .setPort(clonePort)
                .setUsername(cloneUser)
                .setPassword(clonePassword);
        this.cloneConfig = cloneBuilder.create();

        this.sourceConfig.setUrl(DatasourceConnectionFactory.getJDBCUrl(this.sourceConfig));
        this.sourceConfig.setTableName(sourceTableName);

        this.cloneConfig.setUrl(DatasourceConnectionFactory.getJDBCUrl(this.cloneConfig));
        this.cloneConfig.setTableName(cloneTableName);


        if(!StringUtil.isNullOrEmpty(this.hdfsSourceDataDir)&&!this.hdfsSourceDataDir.substring(hdfsSourceDataDir.length()-1).equals("/"))
            this.hdfsSourceDataDir += "/";
        if(!StringUtil.isNullOrEmpty(this.tempDirectory)&&!this.tempDirectory.substring(tempDirectory.length()-1).equals("/"))
            this.tempDirectory += "/";
        if(this.getCloneConfig().getDataSourceType().equals(DataSourceType.HUDI)){
            if(StringUtil.isNullOrEmpty(this.sparkCustomCommand)){
                throw new TCMException("if you set cloneDataType is HUID , the 'spark.custom.command' should set");
            }else if(!this.hoodieTableType.equals("MERGE_ON_READ") && !this.hoodieTableType.equals("COPY_ON_WRITE")){
                throw new TCMException("if you set cloneDataType is HUID , the 'hudi.table.type' should be 'MERGE_ON_READ' or 'COPY_ON_WRITE'");
            }
        }

        return this;
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public String getHdfsSourceDataDir() {
        return hdfsSourceDataDir;
    }

    public String getHdfsCloneDataPath() {
        return hdfsCloneDataPath;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getHoodieTableType() {
        return hoodieTableType;
    }

    public String getSparkCustomCommand() {
        return sparkCustomCommand;
    }

    public Boolean getGetSourceTableSQL() {
        return getSourceTableSQL;
    }

    public Boolean getGetCloneTableSQL() {
        return getCloneTableSQL;
    }

    public Boolean getExecuteExportScript() {
        return executeExportScript;
    }

    public Boolean getExecuteImportScript() {
        return executeImportScript;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public Boolean getCreateTableInClone() {
        return createTableInClone;
    }

    public String getMemsqlColumnStore() {
        return memsqlColumnStore;
    }

    public Boolean getDeleteCache() {
        return deleteCache;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Boolean getDebug() {
        return debug;
    }
}
