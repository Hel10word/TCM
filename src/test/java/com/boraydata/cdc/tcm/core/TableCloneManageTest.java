package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;
import org.junit.jupiter.api.Test;

/** use manager to clone table for test
 * @author bufan
 * @data 2021/8/26
 */
class TableCloneManageTest {

    //========================== MySQL ===============================
    DatabaseConfig configMySQL = TestDataProvider.MySQLConfig;

    //========================== PgSQL ===============================
    DatabaseConfig configPGSQL = TestDataProvider.PostgreSQLConfig;

    // create the table clone manager
    TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
    TableCloneManageContext tcmc = tcmcBuilder
            .setTcmConfig(TestDataProvider.getTCMConfig(configPGSQL,configPGSQL))
            .create();
    TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmc);
//    String sourceName = "lineitem_test";
    String sourceName = "lineitem_sf10";
    String cloneName = sourceName+"_clone";
//    String cloneName = "numeric_types_pgsql_clone";

    // 获取 在 CreateConfig 上创建表的语句
    @Test
    public void testGetDatabaseTable(){
        // 获取 SourceConfig 中表的信息 里面包含了 映射到 TCM 的数据类型
        Table sourceTable = tcm.createSourceMappingTable(sourceName);
        Table tempTable = tcmc.getTempTable();
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneName);

        String sourceTableSQL = MappingToolFactory.create(tcmc.getSourceConfig().getDataSourceEnum()).getCreateTableSQL(sourceTable);
        String tempTableSQL = "";
        if(tempTable != null)
            tempTableSQL = MappingToolFactory.create(tcmc.getSourceConfig().getDataSourceEnum()).getCreateTableSQL(tempTable);
        String cloneTableSQL = MappingToolFactory.create(tcmc.getCloneConfig().getDataSourceEnum()).getCreateTableSQL(cloneTable);

        System.out.println(sourceTable.outTableInfo());
//        if(tempTable != null)
//          System.out.println(tempTable.getTableInfo());
//        System.out.println(cloneTable.getTableInfo());

//        System.out.println("sourceTableSQL:\n"+sourceTableSQL);
//        System.out.println("\n\ntempTableSQL:\n"+tempTableSQL);
//        System.out.println("\n\ncloneTableSQL:\n"+cloneTableSQL);
    }

    // 获取在同种类型的数据库建表 的 SQL
    @Test
    public void testCreateTableInSameDB(){
        Table lineitem = tcm.createSourceMappingTable(sourceName);

        Table lineitem_clone = tcm.createCloneTable(lineitem, cloneName);

        boolean tableInDatasource = tcm.createTableInDatasource();
        if(tableInDatasource)
            System.out.println("Success!!");
    }

}