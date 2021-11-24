//package com.boraydata.tcm.db2hive;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.Properties;
//
///**
// * TODO
// *
// * @date: 2021/1/13
// * @author: hatter
// **/
//public class InitializationTool {
//
//    private static final Logger log = LoggerFactory.getLogger(InitializationTool.class);
//
//    public static void main(String[] args) throws IOException, InterruptedException {
//
//        if (args.length < 1 || args[0].length() < 1) {
//            System.out.println("Parameter error.");
//            return;
//        }
//
//        Properties props = new Properties();
//        props.load(new FileInputStream(args[0]));
//
//        String csvPath = props.getProperty("source.csv");
//        String pgConnUrl = props.getProperty("source.jdbc.connect");
//        String pgUser = props.getProperty("source.user");
//        String pgPassword = props.getProperty("source.password");
//        String pgDatabase = props.getProperty("source.database");
//        String pgTable = props.getProperty("source.table");
//        String primaryKey = props.getProperty("hive.primary.key");
//        String partitionKey = props.getProperty("hive.partition.key", "_hoodie_date");
//        String hdfsPath = props.getProperty("hive.hdfs.path");
//        String hiveTable = props.getProperty("hive.table");
//        String hoodieTableType = props.getProperty("hive.table.type", "MERGE_ON_READ");
//        String hiveUrl = props.getProperty("hive.jdbc.connect");
//        String hiveUser = props.getProperty("hive.user", null);
//        String hivePassword = props.getProperty("hive.password", null);
//        String hiveDatabase = props.getProperty("hive.database");
//        Boolean nonPartitioned = Boolean.parseBoolean(props.getProperty("hive.non.partitioned", "false").toLowerCase());
//        Boolean hiveMultiPartitionKeys = Boolean.parseBoolean(props.getProperty("hive.multi.partition.keys", "false").toLowerCase());
//
//        String parallel = props.getProperty("parallel", "8");
//        String cdcHome = props.getProperty("cdc.home", null);
//
//        if (primaryKey == null || primaryKey.length() < 1) {
//            System.out.println("Parameter error. `hive.primary.key` is empty");
//            return;
//        }
//
//        Parameter parameter = new Parameter(pgConnUrl, pgUser, pgPassword, pgDatabase, pgTable, parallel, primaryKey,
//                partitionKey, csvPath, hdfsPath, hiveTable, hoodieTableType, hiveUrl, hiveUser, hivePassword,
//                hiveDatabase, nonPartitioned, hiveMultiPartitionKeys, cdcHome);
//
//        HudiBulkInsert bulkInsert = new HudiBulkInsert(parameter);
//        long t1 = System.currentTimeMillis();
//        // read source2
//        bulkInsert.avroGenerator();
//        bulkInsert.configGenerator();
//
//        long t2 = System.currentTimeMillis();
//
//        // write sink
//        bulkInsert.insertHDFS();
//
//        long t3 = System.currentTimeMillis();
//
//        // sync hive
//        bulkInsert.syncHive();
//
//        long t4 = System.currentTimeMillis();
//
//        System.out.println("read schema time : " + (t2 - t1)/1000);
//        System.out.println("write hdfs  time : " + (t3 - t2)/1000);
//        System.out.println("sync hive   time : " + (t4 - t3)/1000);
//    }
//}
