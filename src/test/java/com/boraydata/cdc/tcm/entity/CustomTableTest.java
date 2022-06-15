package com.boraydata.cdc.tcm.entity;


import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;

/**
 * @author bufan
 * @date 2022/4/7
 */
class CustomTableTest {
    String FILEPATH = TestDataProvider.CUSTOM_FILE_PATH;

    @Test
    void writeCustomFile() throws IOException {
        Table customTable = TestDataProvider.getCustomTable();
//        customTable.setColumns(TestDataProvider.getColumns(null));
        LinkedList<String> primaryKeys = new LinkedList<>();
        primaryKeys.add("col_tinyint");
        primaryKeys.add("col_smallint");
        customTable.setPrimaryKeys(primaryKeys);

        String jsonStr = JacksonUtil.toJson(customTable);

        System.out.println(jsonStr);
        FileUtil.writeMsgToFile(jsonStr,FILEPATH);
    }

    @Test
    public void readCustomFile() {

        Table customTable = JacksonUtil.filePathToObject(FILEPATH,Table.class);
        customTable.setTableName("customTable");

        System.out.println(customTable.outTableInfo());

        MappingTool mysqlTool = MappingToolFactory.create(DataSourceEnum.MYSQL);
        assert mysqlTool != null;
        System.out.println("\n"+mysqlTool.getCreateTableSQL(mysqlTool.createCloneMappingTable(customTable.setTableName("mysql_Table"))));

        MappingTool postgresqlTool = MappingToolFactory.create(DataSourceEnum.POSTGRESQL);
        assert postgresqlTool != null;
        System.out.println("\n"+postgresqlTool.getCreateTableSQL(postgresqlTool.createCloneMappingTable(customTable.setTableName("postgresql_Table"))));

        MappingTool sqlserverTool = MappingToolFactory.create(DataSourceEnum.SQLSERVER);
        assert sqlserverTool != null;
        System.out.println("\n"+sqlserverTool.getCreateTableSQL(sqlserverTool.createCloneMappingTable(customTable.setTableName("sqlserver_Table"))));


    }

}