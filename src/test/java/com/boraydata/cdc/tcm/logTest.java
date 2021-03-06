package com.boraydata.cdc.tcm;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.core.util.JsonUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * https://logging.apache.org/log4j/2.x/manual/lookups.html
 * https://docs.oracle.com/javase/7/docs/technotes/guides/jndi/jndi-rmi.html
 * https://issues.apache.org/jira/projects/LOG4J2/issues/LOG4J2-3202?filter=addedrecently
 * @author bufan
 * @date 2021/12/13
 */
public class logTest {

    private static final Logger logger = LoggerFactory.getLogger(logTest.class);

    @Test
    public void logOut(){
//        System.out.println(DataSourceEnum.MYSQL.SQL_TableInfoByTableName);
        System.out.println(DataSourceEnum.POSTGRESQL.SQL_TableInfoByTableName);
        logger.info("${java:vm}");
    }

    @Test
    void name() {
        logger.info("test info");
        logger.warn("test warn");
        logger.error("test error");
    }
}
