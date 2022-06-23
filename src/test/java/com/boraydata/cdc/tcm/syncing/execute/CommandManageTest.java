package com.boraydata.cdc.tcm.syncing.execute;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @date 2021/9/27
 */
class CommandManageTest {

    // 1、创建 Source 数据源信息
    DatabaseConfig sourceConfig = TestDataProvider.MySQLConfig;

    //2、 创建 Clone 数据源信息
    DatabaseConfig cloneConfig = TestDataProvider.PostgreSQLConfig;


    @Test
    public void syncTableDataByTableNameTest1(){
        String filePath = "/usr/local/";
        String tableName = "item";
        StringBuilder sb = new StringBuilder(filePath);
        sb.append(tableName).append("_")
                .append("MySQL").append("_to_")
                .append("PostgreSQL");
        String csvFilePath = sb.append(".csv").toString();
        String loadShellFilePath = csvFilePath.replace(".csv","_load.sh");
        String exportShellFilePath = csvFilePath.replace(".csv","_export.sh");
        System.out.println(csvFilePath);
        System.out.println(loadShellFilePath);
        System.out.println(exportShellFilePath);
    }




}