package com.boraydata.cdc.tcm.syncing;


import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

        System.out.println("generateExportSQLByMysqlDump:\n"+tool.generateExportSQLByMysqlDump(tcmc));
        System.out.println("generateExportSQLByMySQLShell:\n"+tool.generateExportSQLByMySQLShell(tcmc));
        System.out.println("generateExportSQLByShell:\n"+tool.generateExportSQLByShell(tcmc));
        tool.generateLoadSQLByJDBC(tcmc);
        System.out.println("generateLoadSQLByJDBC:\n"+tcmc.getLoadShellContent());
        System.out.println("generateLoadSQLByMySQLShell:\n"+tool.generateLoadSQLByMySQLShell(tcmc));
        System.out.println("generateLoadSQLByShell:\n"+tool.generateLoadSQLByShell(tcmc));
    }


}