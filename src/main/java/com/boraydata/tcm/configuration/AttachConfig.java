package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;

import java.util.Properties;

/**
 * @author bufan
 * @data 2021/11/4
 */
public class AttachConfig {

    private String sourceJdbcConnect;
    private String sourceDatabaseName;
    private String sourceDataType;
    private String sourceHost;
    private String sourcePort;
    private String sourceUser;
    private String sourcePassword;

    private String cloneJdbcConnect;
    private String cloneDatabaseName;
    private String cloneDataType;
    private String cloneHost;
    private String clonePort;
    private String cloneUser;
    private String clonePassword;

    private String sourceTableName;
    private String cloneTableName;

    private String[] colNames;
    private String tempDirectory;
    private String hdfsCsvDir;
    private String hudiHdfsPath;
    private String hudiPrimaryKey;
    private String hudiPartitionKey;
    private String hoodieTableType;
    private Boolean hiveNonPartitioned;
    private Boolean hiveMultiPartitionKeys;
    private String parallel;

    private String tempTableName;
    private String tempTableSQL;
    private String csvFileName;
    private String hdfsCsvPath;
    private String localCsvPath;
    private String exportShellPath;
    private String loadShellPath;
    private String loadExpendScriptPath;

    private String delimiter;
    private Boolean debug;

    private AttachConfig() {
        //
    }
    private static AttachConfig instance = new AttachConfig();

    public static AttachConfig getInstance(){
        return instance;
    }

    public AttachConfig loadLocalConfig(Properties props){

        //======================================================    DatabaseConfig   ==================================================
        // source Connect Config
        this.sourceDatabaseName = props.getProperty("sourceDatabaseName");
        this.sourceDataType = props.getProperty("sourceDataType");
        this.sourceHost = props.getProperty("sourceHost");
        this.sourcePort = props.getProperty("sourcePort");
        this.sourceUser = props.getProperty("sourceUser");
        this.sourcePassword = props.getProperty("sourcePassword");

        //======================================================    DatabaseConfig   ==================================================

        // clone Connect Config
        this.cloneDatabaseName = props.getProperty("cloneDatabaseName");
        this.cloneDataType = props.getProperty("cloneDataType");
        this.cloneHost = props.getProperty("cloneHost");
        this.clonePort = props.getProperty("clonePort");
        this.cloneUser = props.getProperty("cloneUser");
        this.clonePassword = props.getProperty("clonePassword");

        // 'mysql', 'postgresql', 'hive'
        if(this.sourceDataType.equalsIgnoreCase("mysql")) {
            this.sourceDataType = DataSourceType.MYSQL.toString();
        }else if (this.sourceDataType.equalsIgnoreCase("postgresql")){
            this.sourceDataType = DataSourceType.POSTGRES.toString();
        }else if (this.sourceDataType.equalsIgnoreCase("hudi")) {
            throw new TCMException("sourceDataType unable to be");
        }else
            throw new TCMException("pls check configFile,sourceDataType cannot be the '"+this.sourceDataType+'\'');

        if(this.cloneDataType.equalsIgnoreCase("mysql")) {
            this.cloneDataType = DataSourceType.MYSQL.toString();
        }else if (this.cloneDataType.equalsIgnoreCase("postgresql")) {
            this.cloneDataType = DataSourceType.POSTGRES.toString();
        }else if (this.cloneDataType.equalsIgnoreCase("hudi")) {
            this.cloneDataType = DataSourceType.HUDI.toString();
        }else
            throw new TCMException("pls check configFile,cloneDataType cannot be the '"+this.cloneDataType+'\'');


        //======================================================    AttachConfig   ==================================================

        this.sourceTableName = props.getProperty("sourceTable");
        this.cloneTableName = props.getProperty("cloneTable",sourceTableName);
        // save temp files directory
        this.tempDirectory = props.getProperty("tempDirectory","./TCM-Temp/");
        // put csv file in HDFS Path
        this.hdfsCsvDir = props.getProperty("hdfs.csv.dir");
        if(!this.hdfsCsvDir.substring(hdfsCsvDir.length()-1).equals("/"))
            this.hdfsCsvDir += "/";
        // Hive Path In HDFS
        this.hudiHdfsPath = props.getProperty("hudi.hdfs.path");
        this.hudiPrimaryKey = props.getProperty("hudi.primary.key");
        this.hudiPartitionKey = props.getProperty("hudi.partition.key","_hoodie_date");
        this.hoodieTableType = props.getProperty("hive.table.type","MERGE_ON_READ");
        this.hiveNonPartitioned = Boolean.parseBoolean(props.getProperty("hive.non.partitioned", "false").toLowerCase());
        this.hiveMultiPartitionKeys = Boolean.parseBoolean(props.getProperty("hive.multi.partition.keys", "false").toLowerCase());
        this.parallel = props.getProperty("parallel","8");

        // delimiter field in csv
        this.delimiter = props.getProperty("delimiter","|");
        // Control the output of process information
        this.debug = Boolean.parseBoolean(props.getProperty("debug","false").toLowerCase());
        return this;
    }

    @Override
    public String toString() {
        return "AttachConfig{" +
                "\n sourceJdbcConnect='" + sourceJdbcConnect + '\'' +
                "\n sourceDatabaseName='" + sourceDatabaseName + '\'' +
                "\n sourceDataType='" + sourceDataType + '\'' +
                "\n sourceHost='" + sourceHost + '\'' +
                "\n sourcePort='" + sourcePort + '\'' +
                "\n sourceUser='" + sourceUser + '\'' +
                "\n sourcePassword='" + sourcePassword + '\'' +
                "\n cloneJdbcConnect='" + cloneJdbcConnect + '\'' +
                "\n cloneDatabaseName='" + cloneDatabaseName + '\'' +
                "\n cloneDataType='" + cloneDataType + '\'' +
                "\n cloneHost='" + cloneHost + '\'' +
                "\n clonePort='" + clonePort + '\'' +
                "\n cloneUser='" + cloneUser + '\'' +
                "\n clonePassword='" + clonePassword + '\'' +
                "\n sourceTableName='" + sourceTableName + '\'' +
                "\n cloneTableName='" + cloneTableName + '\'' +
                "\n tempDirectory='" + tempDirectory + '\'' +
                "\n hdfsCsvDir='" + hdfsCsvDir + '\'' +
                "\n hiveHdfsPath='" + hudiHdfsPath + '\'' +
                "\n hivePrimaryKey='" + hudiPrimaryKey + '\'' +
                "\n hivePartitionKey='" + hudiPartitionKey + '\'' +
                "\n hoodieTableType='" + hoodieTableType + '\'' +
                "\n hiveNonPartitioned=" + hiveNonPartitioned +
                "\n hiveMultiPartitionKeys=" + hiveMultiPartitionKeys +
                "\n parallel='" + parallel + '\'' +
                "\n tempTableName='" + tempTableName + '\'' +
                "\n csvFileName='" + csvFileName + '\'' +
                "\n hdfsCsvPath='" + hdfsCsvPath + '\'' +
                "\n localCsvPath='" + localCsvPath + '\'' +
                "\n exportShellPath='" + exportShellPath + '\'' +
                "\n loadShellPath='" + loadShellPath + '\'' +
                "\n loadExpendScriptPath='" + loadExpendScriptPath + '\'' +
                "\n delimiter='" + delimiter + '\'' +
                "\n debug=" + debug +
                '}';
    }

    public String getRealSourceTableName(){
        if(tempTableName != null && tempTableName.length()>0)
            return tempTableName;
        return sourceTableName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public AttachConfig setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
        return this;
    }

    public String getCloneTableName() {
        return cloneTableName;
    }

    public AttachConfig setCloneTableName(String cloneTableName) {
        this.cloneTableName = cloneTableName;
        return this;
    }

    public String[] getColNames() {
        return colNames;
    }

    public AttachConfig setColNames(String[] colNames) {
        this.colNames = colNames;
        return this;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public AttachConfig setTempDirectory(String tempDirectory) {
        this.tempDirectory = tempDirectory;
        return this;
    }

    public String getHdfsCsvDir() {
        return hdfsCsvDir;
    }

    public AttachConfig setHdfsCsvDir(String hdfsCsvDir) {
        this.hdfsCsvDir = hdfsCsvDir;
        return this;
    }

    public String getHudiHdfsPath() {
        return hudiHdfsPath;
    }

    public AttachConfig setHudiHdfsPath(String hudiHdfsPath) {
        this.hudiHdfsPath = hudiHdfsPath;
        return this;
    }

    public String getHudiPrimaryKey() {
        return hudiPrimaryKey;
    }

    public AttachConfig setHudiPrimaryKey(String hudiPrimaryKey) {
        this.hudiPrimaryKey = hudiPrimaryKey;
        return this;
    }

    public String getHudiPartitionKey() {
        return hudiPartitionKey;
    }

    public AttachConfig setHudiPartitionKey(String hudiPartitionKey) {
        this.hudiPartitionKey = hudiPartitionKey;
        return this;
    }

    public String getHoodieTableType() {
        return hoodieTableType;
    }

    public AttachConfig setHoodieTableType(String hoodieTableType) {
        this.hoodieTableType = hoodieTableType;
        return this;
    }

    public Boolean getHiveNonPartitioned() {
        return hiveNonPartitioned;
    }

    public AttachConfig setHiveNonPartitioned(Boolean hiveNonPartitioned) {
        this.hiveNonPartitioned = hiveNonPartitioned;
        return this;
    }

    public Boolean getHiveMultiPartitionKeys() {
        return hiveMultiPartitionKeys;
    }

    public AttachConfig setHiveMultiPartitionKeys(Boolean hiveMultiPartitionKeys) {
        this.hiveMultiPartitionKeys = hiveMultiPartitionKeys;
        return this;
    }

    public String getParallel() {
        return parallel;
    }

    public AttachConfig setParallel(String parallel) {
        this.parallel = parallel;
        return this;
    }

    public String getTempTableName() {
        return tempTableName;
    }

    public AttachConfig setTempTableName(String tempTableName) {
        this.tempTableName = tempTableName;
        return this;
    }

    public String getHdfsCsvPath() {
        return hdfsCsvPath;
    }

    public AttachConfig setHdfsCsvPath(String hdfsCsvPath) {
        this.hdfsCsvPath = hdfsCsvPath;
        return this;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public AttachConfig setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public Boolean getDebug() {
        return debug;
    }

    public AttachConfig setDebug(Boolean debug) {
        this.debug = debug;
        return this;
    }

    public String getLocalCsvPath() {
        return localCsvPath;
    }

    public AttachConfig setLocalCsvPath(String localCsvPath) {
        this.localCsvPath = localCsvPath;
        return this;
    }

    public String getExportShellPath() {
        return exportShellPath;
    }

    public AttachConfig setExportShellPath(String exportShellPath) {
        this.exportShellPath = exportShellPath;
        return this;
    }

    public String getLoadShellPath() {
        return loadShellPath;
    }

    public AttachConfig setLoadShellPath(String loadShellPath) {
        this.loadShellPath = loadShellPath;
        return this;
    }

    public String getLoadExpendScriptPath() {
        return loadExpendScriptPath;
    }

    public AttachConfig setLoadExpendScriptPath(String loadExpendScriptPath) {
        this.loadExpendScriptPath = loadExpendScriptPath;
        return this;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public String getSourceDatabaseName() {
        return sourceDatabaseName;
    }

    public AttachConfig setSourceDatabaseName(String sourceDatabaseName) {
        this.sourceDatabaseName = sourceDatabaseName;
        return this;
    }

    public String getSourceUser() {
        return sourceUser;
    }

    public AttachConfig setSourceUser(String sourceUser) {
        this.sourceUser = sourceUser;
        return this;
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public AttachConfig setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
        return this;
    }

    public String getCloneDatabaseName() {
        return cloneDatabaseName;
    }

    public AttachConfig setCloneDatabaseName(String cloneDatabaseName) {
        this.cloneDatabaseName = cloneDatabaseName;
        return this;
    }


    public String getCloneUser() {
        return cloneUser;
    }

    public AttachConfig setCloneUser(String cloneUser) {
        this.cloneUser = cloneUser;
        return this;
    }

    public String getClonePassword() {
        return clonePassword;
    }

    public AttachConfig setClonePassword(String clonePassword) {
        this.clonePassword = clonePassword;
        return this;
    }

    public String getSourceJdbcConnect() {
        return sourceJdbcConnect;
    }

    public AttachConfig setSourceJdbcConnect(String sourceJdbcConnect) {
        this.sourceJdbcConnect = sourceJdbcConnect;
        return this;
    }

    public String getCloneJdbcConnect() {
        return cloneJdbcConnect;
    }

    public AttachConfig setCloneJdbcConnect(String cloneJdbcConnect) {
        this.cloneJdbcConnect = cloneJdbcConnect;
        return this;
    }

    public String getSourceDataType() {
        return sourceDataType;
    }

    public AttachConfig setSourceDataType(String sourceDataType) {
        this.sourceDataType = sourceDataType;
        return this;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public AttachConfig setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
        return this;
    }

    public String getSourcePort() {
        return sourcePort;
    }

    public AttachConfig setSourcePort(String sourcePort) {
        this.sourcePort = sourcePort;
        return this;
    }

    public String getCloneDataType() {
        return cloneDataType;
    }

    public AttachConfig setCloneDataType(String cloneDataType) {
        this.cloneDataType = cloneDataType;
        return this;
    }

    public String getCloneHost() {
        return cloneHost;
    }

    public AttachConfig setCloneHost(String cloneHost) {
        this.cloneHost = cloneHost;
        return this;
    }

    public String getClonePort() {
        return clonePort;
    }

    public AttachConfig setClonePort(String clonePort) {
        this.clonePort = clonePort;
        return this;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public AttachConfig setCsvFileName(String csvFileName) {
        this.csvFileName = csvFileName;
        return this;
    }

    public String getTempTableSQL() {
        return tempTableSQL;
    }

    public AttachConfig setTempTableSQL(String tempTableSQL) {
        this.tempTableSQL = tempTableSQL;
        return this;
    }
}
