package com.boraydata.tcm;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;

import java.util.Properties;

/**
 * @author bufan
 * @data 2021/11/5
 */
public class TestDataProvider {

    //========================== MySQL ===============================
    public static DatabaseConfig.Builder builderMySQL = new DatabaseConfig.Builder();
    public static DatabaseConfig configMySQL = builderMySQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.148")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    //========================== PgSQL ===============================
    public static DatabaseConfig.Builder builderPGSQL = new DatabaseConfig.Builder();
    public static DatabaseConfig configPGSQL = builderPGSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("")
            .create();

    //========================== Hudi ===============================
    public static DatabaseConfig.Builder builderHudi = new DatabaseConfig.Builder();
    public static DatabaseConfig configHudi = builderHudi
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.HUDI)
            .setHost("192.168.120.67")
            .setPort("10000")
            .setUsername("rapids")
            .setPassword("rapids")
            .create();

    public static TableCloneManageConfig getDefTcmConfig(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        Properties properties = new Properties();

        properties.setProperty("sourceDatabaseName",sourceConfig.getDatabasename());
        properties.setProperty("sourceDataType",sourceConfig.getDataSourceType().toString());
        properties.setProperty("sourceHost",sourceConfig.getHost());
        properties.setProperty("sourcePort",sourceConfig.getPort());
        properties.setProperty("sourceUser",sourceConfig.getUsername());
        properties.setProperty("sourcePassword",sourceConfig.getPassword());
        properties.setProperty("sourceTable","");

        properties.setProperty("cloneDatabaseName",cloneConfig.getDatabasename());
        properties.setProperty("cloneDataType",cloneConfig.getDataSourceType().toString());
        properties.setProperty("cloneHost",cloneConfig.getHost());
        properties.setProperty("clonePort",cloneConfig.getPort());
        properties.setProperty("cloneUser",cloneConfig.getUsername());
        properties.setProperty("clonePassword",cloneConfig.getPassword());
        properties.setProperty("cloneTable","");

        properties.setProperty("hdfs.source.data.path","hdfs://192.168.30.39:9000/HudiTest/");
//        properties.setProperty("hdfs.source.data.path","file:///HudiTest/");
//        properties.setProperty("hdfs.source.data.path","/HudiTest/");
        properties.setProperty("hdfs.clone.data.path","/HudiTest/");
        properties.setProperty("primary.key","id");
        properties.setProperty("partition.key","_hoodie_date");
        properties.setProperty("hudi.table.type","MERGE_ON_READ");
//        properties.setProperty("hudi.table.type","COPY_ON_WRITE");

        properties.setProperty("tempDirectory","./TCM-TempData/");
        properties.setProperty("delimiter","|");
        properties.setProperty("debug","true");

        return TableCloneManageConfig.getInstance().loadLocalConfig(properties);
    }

    public static TableCloneManageContext getTCMC(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        return tcmcBuilder
                .setTcmConfig(getDefTcmConfig(sourceConfig,cloneConfig))
                .create();
    }

    public static TableCloneManage getTCM(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig) {
        return TableCloneManageFactory.createTableCloneManage(getTCMC(sourceConfig,cloneConfig));
    }

    public static DatabaseConfig getConfigMySQL() {
        return configMySQL;
    }

    public static DatabaseConfig getConfigPGSQL() {
        return configPGSQL;
    }

    public static DatabaseConfig getConfigHudi() {
        return configHudi;
    }
}
