package com.boraydata.tcm;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;

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

    public static AttachConfig getDefAttCfg(){
        return AttachConfig.getInstance()
                .setSourceTableName("lineitem")
                .setCloneTableName("lineitem")
                .setTempDirectory("./TCM-Temp/")
                .setHudiPartitionKey("_hoodie_date")
                .setHoodieTableType("MERGE_ON_READ")
                .setHiveMultiPartitionKeys(false)
                .setHiveNonPartitioned(false)
                .setParallel("8")
                .setDelimiter("|")
                .setDebug(false);
    }

    public static TableCloneManageContext getTCMC(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        return tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .setAttachConfig(getDefAttCfg())
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
}
