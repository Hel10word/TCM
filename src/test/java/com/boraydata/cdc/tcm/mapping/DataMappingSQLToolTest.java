package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/11/5
 */
class DataMappingSQLToolTest {
    DataMappingSQLTool tool = new DataMappingSQLTool();

    @Test
    public void getMappingDataSQLTest(){

        Table testTable = TestDataProvider.getTable(DataSourceEnum.MYSQL, "testTable");
//        Table testTable = TestDataProvider.getTable(DataSourceEnum.POSTGRES, "testTable");
        String mappingDataSQL = DataMappingSQLTool.getMappingDataSQL(testTable, DataSourceEnum.HUDI);

        System.out.println(mappingDataSQL);
    }

}