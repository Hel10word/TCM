package com.boraydata.tcm;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://logging.apache.org/log4j/2.x/manual/lookups.html
 * https://docs.oracle.com/javase/7/docs/technotes/guides/jndi/jndi-rmi.html
 * https://issues.apache.org/jira/projects/LOG4J2/issues/LOG4J2-3202?filter=addedrecently
 * @author bufan
 * @data 2021/12/13
 */
public class logTest {

    private static final Logger logger = LoggerFactory.getLogger(logTest.class);

    @Test
    public void logOut(){
        logger.info("${java:vm}");
    }
}
