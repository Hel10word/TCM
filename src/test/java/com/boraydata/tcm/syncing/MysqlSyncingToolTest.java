package com.boraydata.tcm.syncing;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.configuration.TableCloneManageConfig;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/11/8
 */
class MysqlSyncingToolTest {
    MysqlSyncingTool tool = new MysqlSyncingTool();
    TableCloneManageConfig defAttCfg = TestDataProvider.getDefTcmConfig(
            TestDataProvider.getConfigMySQL(),
            TestDataProvider.getConfigPGSQL());


}