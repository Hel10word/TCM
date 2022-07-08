package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;
import org.junit.jupiter.api.Test;

/** use manager to clone table for test
 * @author bufan
 * @date 2021/8/26
 */
class TableCloneManagerTest {

    //========================== MySQL ===============================
    DatabaseConfig configMySQL = TestDataProvider.MySQLConfig;

    //========================== PostgreSQL ===============================
    DatabaseConfig configPostgreSQL = TestDataProvider.PostgreSQLConfig;

    //========================== SQLSERVER ===============================
    DatabaseConfig configSQLServer = TestDataProvider.SQLServerConfig.setCatalog("test_db").setSchema("dbo");

    //========================== SQLSERVER ===============================
    DatabaseConfig configHudi = TestDataProvider.HudiConfig;

    // create the table clone manager
    TableCloneManagerContext.Builder tcmcBuilder = new TableCloneManagerContext.Builder();
    TableCloneManagerContext tcmc = tcmcBuilder
            .setTcmConfig(TestDataProvider.getTCMConfig(configPostgreSQL,configPostgreSQL))
            .create();

    String sourceName = "test";
    String cloneName = sourceName+"_clone";

    // 获取 在 CreateConfig 上创建表的语句
    @Test
    public void testGetDatabaseTable(){
        TableCloneManager tcm = TableCloneManagerFactory.createTableCloneManage(tcmc);
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
        System.out.println(cloneTable.outTableInfo());

        System.out.println("sourceTableSQL:\n"+sourceTableSQL);
//        System.out.println("\n\ntempTableSQL:\n"+tempTableSQL);
        System.out.println("\n\ncloneTableSQL:\n"+cloneTableSQL);
    }

    // 获取在同种类型的数据库建表 的 SQL
    @Test
    public void testCreateTableInSameDB(){
        TableCloneManager tcm = TableCloneManagerFactory.createTableCloneManage(tcmc);
        Table lineitem = tcm.createSourceMappingTable(sourceName);

        Table lineitem_clone = tcm.createCloneTable(lineitem, cloneName);

        boolean tableInDatasource = tcm.createTableInDatasource();
        if(tableInDatasource)
            System.out.println("Success!!");
    }

    @Test
    void fullFunctionTest() {
        String sourceTableName = "lineitem_sf10";
//        String cloneTableName = sourceTableName+"_clone";
        String cloneTableName = "lineitem_sf10_pk";

        TableCloneManagerConfig configTest = TestDataProvider.getTCMConfig(configPostgreSQL, configSQLServer)
                .setOutSourceTableSQL(Boolean.FALSE)
                .setOutCloneTableSQL(Boolean.FALSE)
                .setCreatePrimaryKeyInClone(Boolean.TRUE)
                .setCreateTableInClone(Boolean.FALSE)
                .setExecuteExportScript(Boolean.FALSE)
                .setExecuteLoadScript(Boolean.TRUE)
                .setDelimiter(",")
                .setLineSeparate("\n")
                .setQuote("\"")
                .setEscape("\\")
                .setTempDirectory("/data/cdc_data/fabric-cdc/init/db-hudi-test/")
                .setSourceTableName(sourceTableName)
                .setCloneTableName(cloneTableName);
        TableCloneManagerContext tcmcTest = tcmcBuilder
                .setTcmConfig(configTest)
                .create();
        TableCloneManager tcm = TableCloneManagerFactory.createTableCloneManage(tcmcTest);


        Table sourceMappingTable = tcm.createSourceMappingTable(sourceTableName);
        Table cloneTable = tcm.createCloneTable(sourceMappingTable,cloneTableName);
        tcm.createTableInDatasource();
        tcm.exportTableData();
        tcm.loadTableData();
        tcm.deleteCache();

//        System.out.println(sourceMappingTable.outTableInfo());
//        System.out.println(cloneTable.outTableInfo());


        TableCloneManagerContext context = tcm.getTableCloneManagerContext();
        System.out.println("SourceTableSQL:\n"+context.getSourceTableSQL()+"\n");
        System.out.println("TempleTableSQL:\n"+context.getTempTableSelectSQL()+"\n");
        System.out.println("CloneTableSQL:\n"+context.getCloneTableSQL()+"\n");
        System.out.println("ExportShellContent:\n"+context.getExportShellContent()+"\n");
        System.out.println("LoadShellContent:\n"+context.getLoadShellContent()+"\n");
        System.out.println("LoadDataInHudiScalaScriptContent:\n"+context.getLoadDataInHudiScalaScriptContent()+"\n");
    }

    @Test
    void tableExistsTest() {
        TableCloneManager tcm = TableCloneManagerFactory.createTableCloneManage(tcmc);
//        System.out.println(tcm.tableExists(configMySQL,"t3"));
//        System.out.println(tcm.tableExists(configPostgreSQL,"t3"));
//        System.out.println(tcm.tableExists(configSQLServer,"t3"));
    }
}