package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.syncing.tool.SqlServer;
import org.junit.jupiter.api.Test;


/**
 * @author : bufan
 * @date : 2022/6/13
 */
class SqlServerSyncingToolTest {
    SqlServer tool = new SqlServer();
    DatabaseConfig config = new DatabaseConfig()
            .setDataSourceEnum(DataSourceEnum.SQLSERVER)
            .setHost("192.168.120.237")
            .setPort("1433")
            .setUsername("sa")
            .setPassword("Rapids123*")
            .setDatabaseName("test_db");

    @Test
    public void generateExportSQLByBCPTest(){
        TableCloneManagerContext tcmc = TestDataProvider.getTCMContext(config, config);
        Table table = TestDataProvider.getTable(DataSourceEnum.SQLSERVER, "lineitem");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");
        tcmc.getTcmConfig()
                .setDelimiter("|")
                .setLineSeparate("|\n")
                .setQuote("\"")
                .setEscape(null)
        ;

        System.out.println("generateExportSQLByBCP:\n"+tool.generateExportSQLByBCP(tcmc)+"\n");
        System.out.println("generateLoadSQLByBCP:\n"+tool.generateLoadSQLByBCP(tcmc));
    }

}