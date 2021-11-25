package com.boraydata.tcm.core;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.mapping.MappingToolFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

/** use manager to clone table for test
 * @author bufan
 * @data 2021/8/26
 */
class TableCloneManageTest {

    //========================== MySQL ===============================
    DatabaseConfig.Builder builderMySQL = new DatabaseConfig.Builder();
    DatabaseConfig configMySQL = builderMySQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.148")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    //========================== PgSQL ===============================
    DatabaseConfig.Builder builderPGSQL = new DatabaseConfig.Builder();
    DatabaseConfig configPGSQL = builderPGSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("")
            .create();

    // create the table clone manager
    TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
    TableCloneManageContext tcmc = tcmcBuilder
            .setSourceConfig(configPGSQL)
//            .setSourceConfig(configMySQL)
//            .setCloneConfig(configSpark)
//            .setCloneConfig(configMySQL)
            .setCloneConfig(configPGSQL)
            .setAttachConfig(TestDataProvider.getDefAttCfg())
            .create();
    TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmc);
    String sourceName = "pg_lsn_types_pgsql";
    String cloneName = sourceName+"_clone";
//    String cloneName = "numeric_types_pgsql_clone";

    // 获取 在 CreateConfig 上创建表的语句
    @Test
    public void testGetDatabaseTable(){
        // 获取 SourceConfig 中表的信息 里面包含了 映射到 TCM 的数据类型
        Table sourceTable = tcm.createSourceMappingTable(sourceName);
        Table tempTable = tcm.getTempTable();
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneName);

        String sourceTableSQL = MappingToolFactory.create(tcmc.getSourceConfig().getDataSourceType()).getCreateTableSQL(sourceTable);
        String tempTableSQL = "";
        if(tempTable != null)
            tempTableSQL = MappingToolFactory.create(tcmc.getSourceConfig().getDataSourceType()).getCreateTableSQL(tempTable);
        String cloneTableSQL = MappingToolFactory.create(tcmc.getCloneConfig().getDataSourceType()).getCreateTableSQL(cloneTable);

        sourceTable.outTableInfo();
//        if(tempTable != null)
//            tempTable.outTableInfo();
//        cloneTable.outTableInfo();

//        System.out.println("sourceTableSQL:\n"+sourceTableSQL);
//        System.out.println("\n\ntempTableSQL:\n"+tempTableSQL);
        System.out.println("\n\ncloneTableSQL:\n"+cloneTableSQL);
    }

    @Test
    public void testCreateTableInSameDB(){
        Table lineitem = tcm.createSourceMappingTable(sourceName);

        Table lineitem_clone = tcm.createCloneTable(lineitem, cloneName);

        boolean tableInDatasource = tcm.createTableInDatasource();
        if(tableInDatasource)
            System.out.println("Success!!");
    }

}