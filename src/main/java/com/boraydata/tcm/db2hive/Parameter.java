//package com.boraydata.tcm.db2hive;
//
///**
// * TODO
// *
// * @date: 2021/1/18
// * @author: hatter
// **/
//public class Parameter {
//
//    public final String pgConnUrl;
//    public final String pgUser;
//    public final String pgPassword;
//    public final String pgDatabase;
//    public final String pgTable;
//    public final String parallel;
//    public final String primaryKey;
//    public final String partitionKey;
//    public final String csvPath;
//    public final String hdfsPath;
//    public final String hiveTable;
//    public final String hoodieTableType;
//    public final String hiveUrl;
//    public final String hiveUser;
//    public final String hivePassword;
//    public final String hiveDatabase;
//    public final Boolean nonPartitioned;
//    public final Boolean hiveMultiPartitionKeys;
//    public final String cdcHome;
//
//    public Parameter(String pgConnUrl, String pgUser, String pgPassword, String pgDatabase, String pgTable, String parallel, String primaryKey, String partitionKey, String csvPath, String hdfsPath, String hiveTable, String hoodieTableType, String hiveUrl, String hiveUser, String hivePassword, String hiveDatabase, Boolean nonPartitioned, Boolean hiveMultiPartitionKeys, String cdcHome) {
//        this.pgConnUrl = pgConnUrl;
//        this.pgUser = pgUser;
//        this.pgPassword = pgPassword;
//        this.pgDatabase = pgDatabase;
//        this.pgTable = pgTable;
//        this.parallel = parallel;
//        this.primaryKey = primaryKey;
//        this.partitionKey = partitionKey;
//        this.csvPath = csvPath;
//        this.hdfsPath = hdfsPath;
//        this.hiveTable = hiveTable;
//        this.hoodieTableType = hoodieTableType;
//        this.hiveUrl = hiveUrl;
//        this.hiveUser = hiveUser;
//        this.hivePassword = hivePassword;
//        this.hiveDatabase = hiveDatabase;
//        this.nonPartitioned = nonPartitioned;
//        this.hiveMultiPartitionKeys = hiveMultiPartitionKeys;
//        if (cdcHome == null) {
//            this.cdcHome = System.getenv("CDC_HOME");
//        } else {
//            this.cdcHome = cdcHome;
//        }
//    }
//}
