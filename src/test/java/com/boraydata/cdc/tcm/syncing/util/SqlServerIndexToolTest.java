package com.boraydata.cdc.tcm.syncing.util;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author : bufan
 * @date : 2022/6/22
 */
class SqlServerIndexToolTest {
    String tableName = "lineitem_sf10";
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
        SqlServerIndexTool.checkInformation(tcmContext);
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        Table cloneTable = tcmContext.getCloneTable();
        String primaryKeyName = SqlServerIndexTool.getPrimaryKeyName(dbConfig,cloneTable);
        System.out.println(primaryKeyName);
    }

    @Test
    void getAllNonClusteredIndexListTest() {
        SqlServerIndexTool.checkInformation(tcmContext);
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        String tableName = tcmContext.getCloneTable().getSchemaName()+"."+tcmContext.getCloneTable().getTableName();
        List<String> allNonClusteredIndexList = SqlServerIndexTool.getAllNonClusteredIndexList(dbConfig,tableName);
        System.out.println(allNonClusteredIndexList);
    }

    @Test
    void disableIndexTest() {
        System.out.println(SqlServerIndexTool.disableIndex(tcmContext));
    }

    @Test
    void rebuildIndexTest() {
        System.out.println(SqlServerIndexTool.rebuildIndex(tcmContext));
    }

}