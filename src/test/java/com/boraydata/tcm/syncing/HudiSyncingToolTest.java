package com.boraydata.tcm.syncing;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author bufan
 * @data 2022/3/31
 */
class HudiSyncingToolTest {

    DatabaseConfig.Builder builderHive = new DatabaseConfig.Builder();
    DatabaseConfig configHive = builderHive
            .setDatabasename("test_cdc_hudi")
            .setDataSourceType(DataSourceType.HUDI)
            .setHost("192.168.120.67")
            .setPort("10000")
            .setUsername("")
            .setPassword("")
            .create()
            .setTableName("test");
    HudiSyncingTool syncingTool = new HudiSyncingTool();

    @Test
    public void test() {
        TableCloneManageContext tcmc = TestDataProvider.getTCMC(TestDataProvider.getConfigMySQL(),configHive);

//        System.out.println(configHive.getTableName());
//        System.out.println(tcmc.getCloneConfig().getTableName());
//        System.out.println(tcmc.getTcmConfig().getCloneConfig().getTableName());
//        System.out.println(tcmc.getTcmConfig().getHoodieTableType());

//        syncingTool.deleteOriginTable(tcmc);
    }

}