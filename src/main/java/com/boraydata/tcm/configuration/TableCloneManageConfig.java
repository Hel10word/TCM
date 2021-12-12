package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
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
//    private Boolean hiveNonPartitioned;
//    private Boolean hiveMultiPartitionKeys;
//    private String parallel;

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
//        this.parallel = props.getProperty("parallel","8");

        //======================================================    TCM Tools Config   ==================================================
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


        if(!this.hdfsSourceDataDir.substring(hdfsSourceDataDir.length()-1).equals("/"))
            this.hdfsSourceDataDir += "/";
        if(!this.tempDirectory.substring(tempDirectory.length()-1).equals("/"))
            this.tempDirectory += "/";
        if(this.hoodieTableType != null && this.hoodieTableType.length() != 0){
            if(!this.hoodieTableType.equals("MERGE_ON_READ") && !this.hoodieTableType.equals("COPY_ON_WRITE")){
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
