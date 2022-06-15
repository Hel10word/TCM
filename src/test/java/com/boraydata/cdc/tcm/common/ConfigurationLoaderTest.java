package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author bufan
 * @Description
 * @date 2022/5/18
 */
class ConfigurationLoaderTest {

    String jsonConfigPath = TestDataProvider.JSON_CONFIG_FILE_PATH;
    String propertiesConfigPath = TestDataProvider.PROPERTIES_CONFIG_FILE_PATH;

    @Test
    void writeJson() throws JsonProcessingException {
        TableCloneManagerConfig tcmConfig = TestDataProvider.getTCMConfig(TestDataProvider.MySQLConfig,TestDataProvider.HudiConfig);
        tcmConfig.setSourceTableName("temple_table").setCloneTableName("temple_table");
        tcmConfig = TestDataProvider.setHudiConfig(tcmConfig,"col_varchar");
        String jsonStr = JacksonUtil.toJson(tcmConfig);
        System.out.println(jsonStr);
        FileUtil.writeMsgToFile(jsonStr,jsonConfigPath);
    }

    @Test
    void loadConfigFileTest() throws IOException {
        TableCloneManagerConfig config = ConfigurationLoader.loadConfigFile(jsonConfigPath);
//        TableCloneManagerConfig config = ConfigurationLoader.loadConfigFile(propertiesConfigPath);
        System.out.println(config);
    }
}