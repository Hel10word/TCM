package com.boraydata.tcm.syncing.execute;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/9/27
 */
class CommandManageTest {

    // 1、创建 Source 数据源信息
    DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
    DatabaseConfig sourceConfig = sourceBuilder
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.200")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    //2、 创建 Clone 数据源信息
    DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
    DatabaseConfig cloneConfig = cloneBuilder
            .setDataSourceType(DataSourceType.POSTGRES)
            .setDatabasename("test_db")
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("")
            .create();


    @Test
    public void syncTableDataByTableNameTest1(){
        String filePath = "/usr/local/";
        String tableName = "item";
        StringBuilder sb = new StringBuilder(filePath);
        sb.append(tableName).append("_")
                .append("MySQL").append("_to_")
                .append("PgSQL");
        String csvFilePath = sb.append(".csv").toString();
        String loadShellFilePath = csvFilePath.replace(".csv","_load.sh");
        String exportShellFilePath = csvFilePath.replace(".csv","_export.sh");
        System.out.println(csvFilePath);
        System.out.println(loadShellFilePath);
        System.out.println(exportShellFilePath);
    }




}