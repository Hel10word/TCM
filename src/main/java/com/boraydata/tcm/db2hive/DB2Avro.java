//package com.boraydata.tcm.db2hive;
//
//import org.apache.avro.Schema;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.avro.SchemaConverters;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.Metadata;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
///**
// * TODO
// *
// * @date: 2021/3/15
// * @author: hatter
// **/
//public class DB2Avro {
//
//    public String getAvroSchema(String jdbcConn, String user, String password, String table) {
//
//        SparkSession spark = SparkSession.builder().appName("DBSchemaApp")
//                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[*]").getOrCreate();
//        Dataset<Row> load =
//                spark.read()
//                        .format("jdbc")
//                        .option("url", jdbcConn)
//                        .option("dbtable", table)
//                        .option("user", user)
//                        .option("password", password)
//                        .load();
//        StructType schema = load.schema();
//        schema = schema.add(new StructField("_hoodie_ts", DataTypes.TimestampType, true, Metadata.empty()));
//        schema = schema.add(new StructField("_hoodie_date", DataTypes.StringType, true, Metadata.empty()));
//        spark.close();
//
//        Schema avroSchema = SchemaConverters.toAvroType(schema, false, table + "_record", "boray." + table);
//        return avroSchema.toString();
//    }
//}
