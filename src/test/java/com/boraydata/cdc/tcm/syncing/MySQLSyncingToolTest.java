package com.boraydata.cdc.tcm.syncing;


import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.syncing.tool.Mysql;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @date 2021/11/8
 */
class MySQLSyncingToolTest {
//    Mysql tool = new Mysql();
    MySQLSyncingTool tool = new MySQLSyncingTool();
    DatabaseConfig config = new DatabaseConfig()
            .setDataSourceEnum(DataSourceEnum.MYSQL)
            .setHost("192.168.30.38")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .setDatabaseName("test_db");


    @Test
    public void generateExportSQLByShellTest(){
        TableCloneManagerContext tcmc = TestDataProvider.getTCMContext(config,config);
        Table table = TestDataProvider.getTable(DataSourceEnum.MYSQL, "lineitem_test");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");
        tcmc.getTcmConfig()
                .setDelimiter("|")
                .setLineSeparate("\n")
                .setQuote("\"")
                .setEscape("\\")
        ;


        System.out.println("\ngenerateExportSQLByMySQLShell:");
        System.out.println(tool.generateExportSQLByMySQLShell(tcmc));
        System.out.println("\ngenerateExportSQLByShell:");
        System.out.println(tool.generateExportSQLByShell(tcmc));
        System.out.println("\ngenerateLoadSQLByJDBC:");
        tool.generateLoadSQLByJDBC(tcmc);
        System.out.println(tcmc.getLoadShellContent());
        System.out.println("\ngenerateLoadSQLByMySQLShell:");
        System.out.println(tool.generateLoadSQLByMySQLShell(tcmc));
        System.out.println("\ngenerateLoadSQLByShell:");
        System.out.println(tool.generateLoadSQLByShell(tcmc));
    }


}