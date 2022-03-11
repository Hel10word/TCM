package com.boraydata.tcm.mapping;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.entity.Table;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/11/5
 */
class DataMappingSQLToolTest {





    @Test
    public void testGetSQL(){

        DataMappingSQLTool dmSQLt = new DataMappingSQLTool();
        MappingTool pgMapping = MappingToolFactory.create(DataSourceType.POSTGRES);
        MappingTool mysqlMapping = MappingToolFactory.create(DataSourceType.MYSQL);

        TableCloneManage tcm = TestDataProvider.getTCM(TestDataProvider.configMySQL, TestDataProvider.configPGSQL);
//        TableCloneManage tcm = TestDataProvider.getTCM(TestDataProvider.configPGSQL, TestDataProvider.configMySQL);
        Table sourceTable = tcm.createSourceMappingTable("test");
//        Table cloneTable = tcm.createCloneTable(sourceTable);
//        Table source_cloneTable = mysqlMapping.createCloneMappingTable(cloneTable);
        System.out.println("create Table in MySQL\n"
                +mysqlMapping.getCreateTableSQL(mysqlMapping.createCloneMappingTable(sourceTable))+
                "\n\n");


        System.out.println("\n\n\ncreate Table in PgSQL\n"
                +pgMapping.getCreateTableSQL(pgMapping.createCloneMappingTable(sourceTable))+
                "\n\n");
//        System.out.println(sql+"\n");
        System.out.println(sourceTable.getTableInfo());
//        List list = DatasourceConnectionFactory.executeQuerySQL(TestDataProvider.configPGSQL, sql);
//        list.forEach(System.out::println);
    }





}