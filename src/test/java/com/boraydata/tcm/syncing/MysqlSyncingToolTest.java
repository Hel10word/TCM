package com.boraydata.tcm.syncing;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.configuration.AttachConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author bufan
 * @data 2021/11/8
 */
class MysqlSyncingToolTest {
    MysqlSyncingTool tool = new MysqlSyncingTool();
    AttachConfig defAttCfg = TestDataProvider.getDefAttCfg()
            .setLocalCsvPath("./test.csv");
    @Test
    public void testGetExportCommand(){
        defAttCfg.setTempTableName("tempTable");
        String exportCommand = tool.exportCommand(TestDataProvider.getConfigMySQL(), defAttCfg);
        System.out.println(exportCommand);
    }

    @Test
    public void testGetLoadCommand(){
        String loadCommand = tool.loadCommand(TestDataProvider.getConfigMySQL(), defAttCfg);
        System.out.println(loadCommand);
    }




}