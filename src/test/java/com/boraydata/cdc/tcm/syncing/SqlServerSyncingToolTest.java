package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;


/**
 * @author : bufan
 * @date : 2022/6/13
 */
class SqlServerSyncingToolTest {
    SqlServerSyncingTool tool = new SqlServerSyncingTool();

    @Test
    public void generateExportSQLByBCPTest(){
        TableCloneManagerContext tcmc = TestDataProvider.getTCMContext(TestDataProvider.SQLServerConfig, TestDataProvider.SQLServerConfig);
        Table table = TestDataProvider.getTable(DataSourceEnum.SQLSERVER, "lineitem");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");

        System.out.println("Export:\n"+tool.getExportInfo(tcmc)+"\n");
        System.out.println("Load:\n"+tool.getLoadInfo(tcmc));
    }

}