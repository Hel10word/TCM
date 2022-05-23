package com.boraydata.cdc.tcm.entity;


import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author bufan
 * @data 2022/4/7
 */
class CustomTableTest {
    String FILEPATH = TestDataProvider.CUSTOM_FILE_PATH;

    @Test
    void writeCustomFile() throws IOException {
        Table customTable = TestDataProvider.getCustomTable();

        String jsonStr = JacksonUtil.toJson(customTable);

        System.out.println(jsonStr);
        FileUtil.writeMsgToFile(jsonStr,FILEPATH);
    }

    @Test
    public void readCustomFile() {

        Table customTable = JacksonUtil.filePathToObject(FILEPATH,Table.class);
        customTable.setTableName("customTable");


        MappingTool tool1 = MappingToolFactory.create(DataSourceEnum.MYSQL);
        assert tool1 != null;
        System.out.println(tool1.getCreateTableSQL(tool1.createCloneMappingTable(customTable)));
        MappingTool tool2 = MappingToolFactory.create(DataSourceEnum.POSTGRESQL);
        assert tool2 != null;
        System.out.println(tool2.getCreateTableSQL(tool2.createCloneMappingTable(customTable)));

        System.out.println(customTable.outTableInfo());
    }

}