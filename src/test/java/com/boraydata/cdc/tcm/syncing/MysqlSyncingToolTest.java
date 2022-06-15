package com.boraydata.cdc.tcm.syncing;


import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @date 2021/11/8
 */
class MysqlSyncingToolTest {
    MysqlSyncingTool tool = new MysqlSyncingTool();




    @Test
    public void generateExportSQLByShellTest(){
        TableCloneManagerContext tcmc = TestDataProvider.getTCMContext(TestDataProvider.MySQLConfig, TestDataProvider.MySQLConfig);
        Table table = TestDataProvider.getTable(DataSourceEnum.MYSQL, "lineitem_test");
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