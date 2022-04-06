package com.boraydata.tcm.syncing;


import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/11/8
 */
class MysqlSyncingToolTest {
    MysqlSyncingTool tool = new MysqlSyncingTool();




    @Test
    public void generateExportSQLByShellTest(){
        TableCloneManageContext tcmc = TestDataProvider.getTCMC(TestDataProvider.getConfigMySQL(), TestDataProvider.getConfigMySQL());
        Table table = TestDataProvider.getTable(DataSourceType.MYSQL, "lineitem_test");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");


//        tool.generateExportSQLByShell(tcmc);
//        tool.generateLoadSQLByMysqlShell(tcmc);


        System.out.println(tcmc.getExportShellContent());
        System.out.println(tcmc.getLoadShellContent());
    }



}