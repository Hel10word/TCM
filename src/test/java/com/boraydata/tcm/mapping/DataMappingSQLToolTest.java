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

    DataMappingSQLTool dmSQLt = new DataMappingSQLTool();
    @Test
    public void testGetSQL(){

        TableCloneManage tcm = TestDataProvider.getTCM(TestDataProvider.configMySQL, TestDataProvider.configPGSQL);
        Table boolean_pgsql = tcm.createSourceMappingTable("byte_types_mysql");
        String sql = dmSQLt.getMappingDataSQL(boolean_pgsql, DataSourceType.POSTGRES);
        System.out.println(sql+"\n");
        System.out.println(boolean_pgsql.getTableInfo());
//        List list = DatasourceConnectionFactory.executeQuerySQL(TestDataProvider.configPGSQL, sql);
//        list.forEach(System.out::println);
    }

}