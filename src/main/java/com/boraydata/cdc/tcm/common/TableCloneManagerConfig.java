package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Define the configuration information of TableCloneManager
 * @author bufan
 * @date 2021/11/4
 */
@JsonPropertyOrder({
        "sourceConfig",
        "cloneConfig",
        "redis",
        "sourceTableName",
        "cloneTableName",
        "sourceTableNames",
        "cloneTableNames",

//        "customSchemaFilePath",
        "customTables",

        "hdfsSourceDataDir",
        "hdfsCloneDataPath",
        "primaryKey",
        "partitionKey",
        "hoodieTableType",
        "nonPartition",
        "multiPartitionKeys",

        "sparkCustomCommand",

        "useCustomTables",
        "outSourceTableSQL",
        "outCloneTableSQL",
        "createTableInClone",
        "createPrimaryKeyInClone",
        "executeExportScript",
        "executeLoadScript",
        "csvFileName",
        "deleteCache",
        "tempDirectory",
        "delimiter",
        "lineSeparate",
        "quote",
        "escape",
        "debug",
})
public class TableCloneManagerConfig {
    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private String sourceTableName;
    private String cloneTableName;
    private RedisConfig redis;

    private LinkedList<String> sourceTableNames;
    private LinkedList<String> cloneTableNames;

    /**
     * support have not regular metadata data type such as kafka and hadoop
     * e.g. ./customTable.json
     */
    private String customSchemaFilePath;
    /**
     * specifies the custom format {@link com.boraydata.cdc.tcm.entity.Table}
     */
    private Table customTable;
    private LinkedList<Table> customTables;

    /**
     * CSV file save to HDFS Path.
     * default: N/A (Required)
     * e.g. hdfs:///HudiTest/
     */
    private String hdfsSourceDataDir;

    /**
     * hudi table data save path in HDFS，please make sure this directory is empty
     * default: N/A (Required)
     * e.g. hdfs:///HudiTest/demo_Hudi
     */
    private String hdfsCloneDataPath;
    /**
     * hudi Record key field. Normally the sourceTable primary key or UUID.
     * default: N/A (Required)
     * e.g. id
     */
    private String primaryKey;
    /**
     * hudi Partition path field. Value to be used at the partitionPath component of HoodieKey.
     * default: _hoodie_date (Optional)
     */
    private String partitionKey = "_hoodie_date";
    /**
     * hudi Type of table to write.
     * Optional Value:COPY_ON_WRITE、MERGE_ON_READ
     * default: MERGE_ON_READ (Optional)
     */
    private String hoodieTableType = "MERGE_ON_READ";
    /**
     * hudi table should or should not be multiPartitioned.
     * whether have hive partition field, if false,default use time field to partition
     * default: false (Optional)
     */
    private Boolean nonPartition = false;
    /**
     * hudi table should or should not be multiPartitioned.
     * whether to support hive multi-partition field
     * default: false (Optional)
     */
    private Boolean multiPartitionKeys = false;
    /**
     * start spark job command
     */
    private String sparkCustomCommand;

    /**
     * Prefer to use Custom Tables information as source table
     * default: false (Optional)
     */
    private Boolean useCustomTables = false;
    /**
     *  record the create table statement of the Source table, .sql file save in 'tempDirectory'
     *  default: false (Optional)
     */
    private Boolean outSourceTableSQL = false;
    /**
     * record the create table statement of the Clone table, .sql file save in 'tempDirectory'
     * default: false (Optional)
     */
    private Boolean outCloneTableSQL = false;
    /**
     * whether create table in Clone,if false ,need to make sure the {@link #getCloneTableName()} table exists
     * default: true (Optional)
     */
    private Boolean createTableInClone = true;
    /**
     * whether create table in Clone witch primary keys
     * default: true (Optional)
     */
    private Boolean createPrimaryKeyInClone = true;
    /**
     * whether execute export csv file script from source.
     * default: true (Optional)
     */
    private Boolean executeExportScript = true;
    /**
     * whether execute load csv file script to clone.
     * default: true (Optional)
     */
    private Boolean executeLoadScript = true;
    /**
     * assign export csv file name,
     * default name format: "Export_from_" + sourceType + "_" + table.getTableName() + ".csv"
     */
    private String csvFileName;

    /**
     * The TCM tools generated temp file directory.
     * default: ./TCM-Temp (Optional)
     */
    private String tempDirectory = "./TCM-Temp";
    /**
     * whether delete temp file in 'tempDirectory'
     * default: true (Optional)
     */
    private Boolean deleteCache = true;
    /**
     * Separator for each field of CSV file
     * default: | (Optional)
     */
    private String delimiter = "|";
    /**
     * Separator for each line of CSV file
     * default: \n (Optional)
     */
    private String lineSeparate = "\n";
    /**
     * quoter each field of CSV file
     * default: " (Optional)
     */
    private String quote = "\"";
    /**
     * escape delimiter of CSV file
     * default: \ (Optional)
     */
    private String escape = "\\";
    /**
     * Whether to output detailed information during the running process
     * default: false (Optional)
     */
    private Boolean debug = false;


    public TableCloneManagerConfig checkConfig(){
        this.sourceConfig = this.sourceConfig.checkConfig();
        DataSourceEnum sourceEnum = this.sourceConfig.getDataSourceEnum();
        if(sourceEnum.equals(DataSourceEnum.HUDI))
            throw new TCMException("SourceDataType unable to be"+sourceEnum);
        if(DataSourceEnum.HADOOP.equals(sourceEnum) || DataSourceEnum.KAFKA.equals(sourceEnum)){
            this.executeExportScript = Boolean.FALSE;
            this.executeLoadScript = Boolean.FALSE;
        }


        this.cloneConfig = cloneConfig.checkConfig();
        DataSourceEnum cloneEnum = this.cloneConfig.getDataSourceEnum();
        if(cloneEnum.equals(DataSourceEnum.HUDI)){
            if(StringUtil.isNullOrEmpty(hdfsSourceDataDir))
                throw new TCMException("if you choose Hudi as sink type,should make sure the hdfsSourceDataDir value is correct，not is "+hdfsSourceDataDir);
            if(StringUtil.isNullOrEmpty(hdfsCloneDataPath))
                throw new TCMException("if you choose Hudi as sink type,should make sure the hdfsCloneDataPath value is correct，not is "+hdfsCloneDataPath);
            if(StringUtil.isNullOrEmpty(sparkCustomCommand))
                throw new TCMException("if you choose Hudi as sink type,should make sure the sparkCustomCommand value is correct，not is "+sparkCustomCommand);
            if(StringUtil.isNullOrEmpty(primaryKey))
                throw new TCMException("if you choose Hudi as sink type,should make sure the primaryKey value is correct，not is "+primaryKey);
            if(!hoodieTableType.equals("MERGE_ON_READ") && !hoodieTableType.equals("COPY_ON_WRITE"))
                throw new TCMException("if you choose Hudi as sink type,should make sure the hoodieTableType value is correct，not is "+hoodieTableType);

            if(hdfsSourceDataDir.charAt(hdfsSourceDataDir.length()-1) != '/')
                this.hdfsSourceDataDir = hdfsSourceDataDir + "/";
            if(hdfsCloneDataPath.charAt(hdfsCloneDataPath.length()-1) != '/')
                this.hdfsCloneDataPath = hdfsCloneDataPath + "/";
        }

        if(null != this.cloneTableNames && !this.cloneTableNames.isEmpty()){
            if(this.sourceTableNames.size() != this.cloneTableNames.size())
                throw new TCMException("the sourceTableNames size is :"+this.sourceTableNames.size() + " But cloneTableNames size is "+this.cloneTableNames.size());
        }else {
            if(StringUtil.isNullOrEmpty(sourceTableName))
                throw new TCMException("the sourceTableName is null,sourceTableName:"+hoodieTableType);
            if(StringUtil.isNullOrEmpty(cloneTableName))
                this.cloneTableName = this.sourceTableName;
        }



        // Custom Table
        if(Objects.isNull(customTable) && Boolean.FALSE.equals(StringUtil.isNullOrEmpty(customSchemaFilePath)))
            this.customTable = JacksonUtil.filePathToObject(customSchemaFilePath,Table.class);
        if(Objects.nonNull(customTable))
            this.customTable.setTableName(sourceTableName).setDataSourceEnum(sourceEnum);

        if(tempDirectory.charAt(tempDirectory.length()-1) != '/')
            this.tempDirectory = tempDirectory + "/";
        return this;
    }




    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public TableCloneManagerConfig setSourceConfig(DatabaseConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public TableCloneManagerConfig setCloneConfig(DatabaseConfig cloneConfig) {
        this.cloneConfig = cloneConfig;
        return this;
    }

    public RedisConfig getRedis() {
        return redis;
    }

    public TableCloneManagerConfig setRedis(RedisConfig redis) {
        this.redis = redis;
        return this;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public TableCloneManagerConfig setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
        return this;
    }

    public String getCloneTableName() {
        return cloneTableName;
    }

    public TableCloneManagerConfig setCloneTableName(String cloneTableName) {
        this.cloneTableName = cloneTableName;
        return this;
    }

    public LinkedList<String> getSourceTableNames() {
        return sourceTableNames;
    }

    public TableCloneManagerConfig setSourceTableNames(LinkedList<String> sourceTableNames) {
        this.sourceTableNames = sourceTableNames;
        return this;
    }

    public LinkedList<String> getCloneTableNames() {
        return cloneTableNames;
    }

    public TableCloneManagerConfig setCloneTableNames(LinkedList<String> cloneTableNames) {
        this.cloneTableNames = cloneTableNames;
        return this;
    }

    public String getCustomSchemaFilePath() {
        return customSchemaFilePath;
    }

    public TableCloneManagerConfig setCustomSchemaFilePath(String customSchemaFilePath) {
        this.customSchemaFilePath = customSchemaFilePath;
        return this;
    }

    public Table getCustomTable() {
        return customTable;
    }

    public TableCloneManagerConfig setCustomTable(Table customTable) {
        this.customTable = customTable;
        return this;
    }

    public LinkedList<Table> getCustomTables() {
        return customTables;
    }

    public TableCloneManagerConfig setCustomTables(LinkedList<Table> customTables) {
        this.customTables = customTables;
        return this;
    }

    public String getHdfsSourceDataDir() {
        return hdfsSourceDataDir;
    }

    public TableCloneManagerConfig setHdfsSourceDataDir(String hdfsSourceDataDir) {
        this.hdfsSourceDataDir = hdfsSourceDataDir;
        return this;
    }

    public String getHdfsCloneDataPath() {
        return hdfsCloneDataPath;
    }

    public TableCloneManagerConfig setHdfsCloneDataPath(String hdfsCloneDataPath) {
        this.hdfsCloneDataPath = hdfsCloneDataPath;
        return this;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public TableCloneManagerConfig setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public TableCloneManagerConfig setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public String getHoodieTableType() {
        return hoodieTableType;
    }

    public TableCloneManagerConfig setHoodieTableType(String hoodieTableType) {
        this.hoodieTableType = hoodieTableType;
        return this;
    }

    public Boolean getNonPartition() {
        return nonPartition;
    }

    public TableCloneManagerConfig setNonPartition(Boolean nonPartition) {
        this.nonPartition = nonPartition;
        return this;
    }

    public Boolean getMultiPartitionKeys() {
        return multiPartitionKeys;
    }

    public TableCloneManagerConfig setMultiPartitionKeys(Boolean multiPartitionKeys) {
        this.multiPartitionKeys = multiPartitionKeys;
        return this;
    }

    public String getSparkCustomCommand() {
        return sparkCustomCommand;
    }

    public TableCloneManagerConfig setSparkCustomCommand(String sparkCustomCommand) {
        this.sparkCustomCommand = sparkCustomCommand;
        return this;
    }

    public Boolean getUseCustomTables() {
        return useCustomTables;
    }

    public TableCloneManagerConfig setUseCustomTables(Boolean useCustomTables) {
        this.useCustomTables = useCustomTables;
        return this;
    }

    public Boolean getOutSourceTableSQL() {
        return outSourceTableSQL;
    }

    public TableCloneManagerConfig setOutSourceTableSQL(Boolean outSourceTableSQL) {
        this.outSourceTableSQL = outSourceTableSQL;
        return this;
    }

    public Boolean getOutCloneTableSQL() {
        return outCloneTableSQL;
    }

    public TableCloneManagerConfig setOutCloneTableSQL(Boolean outCloneTableSQL) {
        this.outCloneTableSQL = outCloneTableSQL;
        return this;
    }

    public Boolean getCreateTableInClone() {
        return createTableInClone;
    }

    public TableCloneManagerConfig setCreateTableInClone(Boolean createTableInClone) {
        this.createTableInClone = createTableInClone;
        return this;
    }

    public Boolean getCreatePrimaryKeyInClone() {
        return createPrimaryKeyInClone;
    }

    public TableCloneManagerConfig setCreatePrimaryKeyInClone(Boolean createPrimaryKeyInClone) {
        this.createPrimaryKeyInClone = createPrimaryKeyInClone;
        return this;
    }

    public Boolean getExecuteExportScript() {
        return executeExportScript;
    }

    public TableCloneManagerConfig setExecuteExportScript(Boolean executeExportScript) {
        this.executeExportScript = executeExportScript;
        return this;
    }

    public Boolean getExecuteLoadScript() {
        return executeLoadScript;
    }

    public TableCloneManagerConfig setExecuteLoadScript(Boolean executeLoadScript) {
        this.executeLoadScript = executeLoadScript;
        return this;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public TableCloneManagerConfig setCsvFileName(String csvFileName) {
        this.csvFileName = csvFileName;
        return this;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public TableCloneManagerConfig setTempDirectory(String tempDirectory) {
        this.tempDirectory = tempDirectory;
        return this;
    }

    public Boolean getDeleteCache() {
        return deleteCache;
    }

    public TableCloneManagerConfig setDeleteCache(Boolean deleteCache) {
        this.deleteCache = deleteCache;
        return this;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public TableCloneManagerConfig setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public String getLineSeparate() {
        return lineSeparate;
    }

    public TableCloneManagerConfig setLineSeparate(String lineSeparate) {
        this.lineSeparate = lineSeparate;
        return this;
    }

    public String getQuote() {
        return quote;
    }

    public TableCloneManagerConfig setQuote(String quote) {
        this.quote = quote;
        return this;
    }

    public String getEscape() {
        return escape;
    }

    public TableCloneManagerConfig setEscape(String escape) {
        this.escape = escape;
        return this;
    }

    public Boolean getDebug() {
        return debug;
    }

    public TableCloneManagerConfig setDebug(Boolean debug) {
        this.debug = debug;
        return this;
    }


}
