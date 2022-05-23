package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.core.TableCloneManageContext;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2022/3/31
 */
class HudiSyncingToolTest {

    DatabaseConfig configHudi = TestDataProvider.HudiConfig.setDatabaseName("test_cdc_hudi");
    HudiSyncingTool syncingTool = new HudiSyncingTool();

    @Test
    public void test() {
        TableCloneManageContext tcmc = TestDataProvider.getTCMContext(TestDataProvider.MySQLConfig, configHudi);

//        System.out.println(configHive.getTableName());
//        System.out.println(tcmc.getCloneConfig().getTableName());
//        System.out.println(tcmc.getTcmConfig().getCloneConfig().getTableName());
//        System.out.println(tcmc.getTcmConfig().getHoodieTableType());

//        syncingTool.deleteOriginTable(tcmc);
    }

    @Test
    void deleteOriginTableTest() {
        TableCloneManageContext tcmc = TestDataProvider.getTCMContext(TestDataProvider.MySQLConfig, configHudi);
        // COPY_ON_WRITE
        tcmc.getTcmConfig().setCloneTableName("temple_table").setHoodieTableType("MERGE_ON_READ");
        syncingTool.deleteOriginTable(tcmc);
    }
}