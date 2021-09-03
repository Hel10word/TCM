package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
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
            .setHost("192.168.30.192")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    //========================== PgSQL ===============================
    DatabaseConfig.Builder builderPGSQL = new DatabaseConfig.Builder();
    DatabaseConfig configPGSQL = builderPGSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.192")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("")
            .create();

    // create the table clone manager
    TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
    TableCloneManageContext tcmc = tcmcBuilder
            .setSourceConfig(configMySQL)
            .setCloneConfig(configPGSQL)
//            .setSourceConfig(configPGSQL)
//            .setCloneConfig(configMySQL)
            .create();
    TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmc);

    // 获取 在 CreateConfig 上创建表的语句
    @Test
    public void testGetDatabaseTable(){
        // 获取 SourceConfig 中表的信息 里面包含了 映射到 TCM 的数据类型
        Table table = tcm.getSourceTable("colume_type");
//        Table table = tcm.getSourceTable("robin_types_full");

        // 创建一个 映射到 CloneConfig 上的表
        Table cloneMappingTable = tcm.getCloneMappingTool().createCloneMappingTable(table);
//        List<Column> columns = table.getColumns();
//        for (Column column : columns)
//            System.out.println(column.toString());
        System.out.println("================ Mapping Table Info\n");
        List<Column> MappingColumns = cloneMappingTable.getColumns();
        for (Column column : MappingColumns)
            System.out.println(column.toString());

        // 输出 在 CloneConfig 上建立映射表的语句
        System.out.println("\n================ Create Table SQL in "+tcm.getCloneConfig().getDataSourceType().name()+" \n");
        System.out.println(tcm.getCloneMappingTool().getCreateTableSQL(cloneMappingTable));
    }
}