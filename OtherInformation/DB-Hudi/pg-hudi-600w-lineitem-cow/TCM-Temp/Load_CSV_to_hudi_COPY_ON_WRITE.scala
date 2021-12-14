import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.types._

val schema = new StructType().add("l_orderkey",IntegerType).add("l_partkey",IntegerType).add("l_suppkey",IntegerType).add("l_linenumber",IntegerType).add("l_quantity",StringType).add("l_extendedprice",StringType).add("l_discount",StringType).add("l_tax",StringType).add("l_returnflag",StringType).add("l_linestatus",StringType).add("l_shipdate",DateType).add("l_commitdate",DateType).add("l_receiptdate",DateType).add("l_shipinstruct",StringType).add("l_shipmode",StringType).add("l_comment",StringType).add("id",IntegerType);

val df = spark.read.schema(schema).option("delimiter", "|").option("escape", "\\").option("quote", "\"").csv("hdfs:///HudiTest/Export_from_POSTGRES_lineitem_clone.csv").withColumn("_hoodie_ts",lit(null).cast(TimestampType)).withColumn("_hoodie_date",lit(null).cast(StringType));

df.write.format("org.apache.hudi").option("hoodie.datasource.write.operation","insert").option("hoodie.datasource.write.table.type","COPY_ON_WRITE").option("hoodie.embed.timeline.server",false).option("hoodie.table.name","lineitem_clone_cow").option("hoodie.datasource.write.recordkey.field","id").option("hoodie.datasource.write.partitionpath.field","_hoodie_date").option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://192.168.120.67:10000/test_cdc_hudi").option("hoodie.datasource.hive_sync.database", "test_cdc_hudi").option("hoodie.datasource.hive_sync.username","rapids").option("hoodie.datasource.hive_sync.password","rapids").option("hoodie.datasource.hive_sync.enable",true).option("hoodie.datasource.hive_sync.table","lineitem_clone_cow").option("hoodie.datasource.hive_sync.partition_fields","_hoodie_date").option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor").option("hoodie.datasource.hive_sync.mode","hms").mode(Append).save("/HudiTest/lineitem_clone_cow");

System.exit(0);