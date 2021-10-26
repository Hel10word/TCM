package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author bufan
 * @data 2021/10/12
 */
class SparkCommandGenerateTest {

    DatabaseConfig.Builder spark = new DatabaseConfig.Builder();
    DatabaseConfig sparkConfig = spark
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.SPARK)
            .setHost("192.168.30.221")
            .setPort("10000")
            .setUsername("")
            .setPassword("")
            .create();

    CommandGenerate commandGenerate = new SparkCommandGenerate();


    @Test
    public void loadCommandTest(){
        String s = this.commandGenerate.loadCommand(sparkConfig, "/usr/local/lineitem_10_mysql.csv", "lineitem_mysql",",");
        System.out.println(s);
    }

}