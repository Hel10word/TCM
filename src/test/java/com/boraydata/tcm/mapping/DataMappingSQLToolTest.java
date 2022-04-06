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
    DataMappingSQLTool tool = new DataMappingSQLTool();

    @Test
    public void getMappingDataSQLTest(){

        Table testTable = TestDataProvider.getTable(DataSourceType.MYSQL, "testTable");
//        Table testTable = TestDataProvider.getTable(DataSourceType.POSTGRES, "testTable");
        String mappingDataSQL = DataMappingSQLTool.getMappingDataSQL(testTable, DataSourceType.HUDI);

        System.out.println(mappingDataSQL);
    }

}