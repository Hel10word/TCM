package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.TableCloneManagerLauncher;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author bufan
 * @date 2021/12/20
 */
public class ConfigLoadTest {

    @Test
    public void getValueTest(){
        try(InputStream in = TableCloneManagerLauncher.class.getClassLoader().getResourceAsStream("./config.properties")){
            Properties properties = new Properties();
            properties.load(in);
            String property = properties.getProperty("spark.custom.command");
            System.out.println(property);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
