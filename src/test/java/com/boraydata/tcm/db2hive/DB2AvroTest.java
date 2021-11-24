//package com.boraydata.tcm.db2hive;
//
//import com.boraydata.tcm.TestDataProvider;
//import com.boraydata.tcm.configuration.DatabaseConfig;
//import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
//import org.junit.jupiter.api.Test;
//
//import static org.junit.jupiter.api.Assertions.*;
//
///**
// * @author bufan
// * @data 2021/11/8
// */
//class DB2AvroTest {
//    DB2Avro db2Avro = new DB2Avro();
//
//    @Test
//    public void testGetAvroSchema(){
//        DatabaseConfig config = TestDataProvider.getConfigPGSQL();
//        String jdbcUrl = DatasourceConnectionFactory.getJDBCUrl(config);
//        String avroSchema = db2Avro.getAvroSchema(jdbcUrl, config.getUsername(), config.getPassword(), "lineitem_pgsql");
//        System.out.println(avroSchema);
//
//    }
//
//}