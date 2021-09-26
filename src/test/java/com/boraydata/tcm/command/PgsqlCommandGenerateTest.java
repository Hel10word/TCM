package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author bufan
 * @data 2021/9/26
 */
class PgsqlCommandGenerateTest {

    DatabaseConfig.Builder pgsql = new DatabaseConfig.Builder();
    DatabaseConfig pgsqlConfig = pgsql
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("postgres")
            .create();

    CommandGenerate commandGenerate = new PgsqlCommandGenerate();
    @Test
    public void exportCommandTest(){
        String s = this.commandGenerate.exportCommand(pgsqlConfig, "/usr/local/lineitem_5_pgsql.csv", "select * from lineitem limit 5;");
        System.out.println(s);
    }
    @Test
    public void loadCommandTest(){
        String s = this.commandGenerate.loadCommand(pgsqlConfig, "/usr/local/lineitem_5_pgsql.csv", "lineitem");
        System.out.println(s);
    }





}