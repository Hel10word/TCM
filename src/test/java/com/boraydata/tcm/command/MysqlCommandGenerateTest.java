package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;


/**
 * @author bufan
 * @data 2021/9/26
 */
class MysqlCommandGenerateTest {
    DatabaseConfig.Builder mysql = new DatabaseConfig.Builder();
    DatabaseConfig mysqlConfig = mysql
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.200")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    CommandGenerate commandGenerate = new MysqlCommandGenerate();
    @Test
    public void exportCommandTest(){
        String s = this.commandGenerate.exportCommand(mysqlConfig, "/usr/local/lineitem_10_mysql.csv", "select * from lineitem limit 10;");
        System.out.println(s);
    }
    @Test
    public void loadCommandTest(){
        String s = this.commandGenerate.loadCommand(mysqlConfig, "/usr/local/lineitem_10_mysql.csv", "lineitem");
        System.out.println(s);
    }

}