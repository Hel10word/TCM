package com.boraydata.cdc.tcm.syncing.util;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author : bufan
 * @date : 2022/6/22
 */
class SqlServerIndexToolTest {
    String tableName = "lineitem_test";
    String catalogName = "test_db";
    String schemaName = "dbo";

    DatabaseConfig dbConfig = TestDataProvider.SQLServerConfig
            .setCatalog(catalogName)
            .setDatabaseName(catalogName)
            .setSchema(schemaName);
        Table table = new Table()
                .setDataSourceEnum(DataSourceEnum.SQLSERVER)
                .setTableName(tableName)
                .setCatalogName(catalogName)
                .setSchemaName(schemaName);

    TableCloneManagerConfig tcmConfig = TestDataProvider.getTCMConfig(dbConfig,dbConfig)
            .setCloneTableName(tableName);
    TableCloneManagerContext tcmContext = TestDataProvider.getTCMContext(tcmConfig)
            .setCloneTable(table);


    @Test
    void fillingPrimaryKeyNameTest() {
        SqlServerIndexTool.fillingPrimaryKeyName(tcmContext);
        System.out.println(tcmContext.getCloneTable().getPrimaryKeyName());
    }

    @Test
    void disableIndexTest() {
        SqlServerIndexTool.fillingPrimaryKeyName(tcmContext);
        System.out.println(SqlServerIndexTool.disableIndex(tcmContext));
    }

    @Test
    void rebuildIndexTest() {
        SqlServerIndexTool.fillingPrimaryKeyName(tcmContext);
        System.out.println(SqlServerIndexTool.rebuildIndex(tcmContext));
    }
}