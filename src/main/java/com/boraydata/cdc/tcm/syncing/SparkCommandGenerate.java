//package com.boraydata.tcm.command;
//
//import DatabaseConfig;
//import Table;
//
///**
// * @author bufan
// * @date 2021/10/12
// *
// * Add SerDe Properties
// * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#:~:text=TBLPROPERTIES%20(%27comment%27%20%3D%20new_comment)%3B-,Add%20SerDe%20Properties,-ALTER%20TABLE%20table_name
// *
// * Loading files into tables
// * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#:~:text=of%20Hive%200.8).-,Loading%20files%20into%20tables,-Hive%20does%20not
// *
// *
// *
// */
//public class SparkCommandGenerate implements CommandGenerate {
//    /**
//     * @描述:
//     * When creating a table in Spark, the default is not definition "ROW FORMAT" to cut up the field
//     * so need to alter the table first
//     * then synchronize the table data
//     * @author: bufan
//     * @date: 2021/10/12 19:43
//     */
//
////    -- load
////      beeline -u jdbc:hive2://192.168.30.221:10000/test_db -n '' -p '' -e "ALTER TABLE lineitem_mysql SET SERDEPROPERTIES ('field.delim'=',');"
////      beeline -u jdbc:hive2://192.168.30.221:10000/test_db -n '' -p '' -e "LOAD DATA LOCAL INPATH '/usr/local/lineitem.csv' INTO TABLE lineitem_mysql;"
//
//
//    public String getConnectCommand(DatabaseConfig config){
//        return String.format("beeline -u jdbc:hive2://%s:%s/%s -n '%s' -p '%s' -e \"?\" 2>&1",
//                config.getHost(),
//                config.getPort(),
//                config.getDatabasename(),
//                config.getUsername(),
//                config.getPassword());
//    }
//
//    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
////        "ALTER TABLE lineitem_mysql SET SERDEPROPERTIES ('field.delim'=',');"
////        "LOAD DATA LOCAL INPATH '/usr/local/lineitem.csv' INTO TABLE lineitem_mysql;"
//        String alterSQL = com.replace("?",
//                "ALTER TABLE "+tableName+" SET SERDEPROPERTIES ('field.delim'='"+delimiter+"');");
//        String loadSQL = com.replace("?",
//                "LOAD DATA LOCAL INPATH '"+filePath+"' INTO TABLE "+tableName+";");
//        return alterSQL+"\n"+loadSQL;
//    }
//
//
//    @Override
//    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter) {
//        return exportCommand(config,filePath,table,delimiter,"");
//    }
//
//    @Override
//    public String exportCommand(DatabaseConfig config, String filePath, Table table, String delimiter, String limit) {
//        return null;
//    }
//
//    @Override
//    public String loadCommand(DatabaseConfig config, String filePath, String tableName, String delimiter) {
//        return completeLoadCommand(
//                getConnectCommand(config),filePath,tableName,delimiter);
//    }
//
//
//
//
//}
