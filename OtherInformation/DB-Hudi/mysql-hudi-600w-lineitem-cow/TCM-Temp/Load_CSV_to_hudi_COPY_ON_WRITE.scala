import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.types._

val schema = new StructType().add("l_orderkey",IntegerType).add("L_PARTKEY",IntegerType).add("L_SUPPKEY",IntegerType).add("L_LINENUMBER",IntegerType).add("L_QUANTITY",StringType).add("L_EXTENDEDPRICE",StringType).add("L_DISCOUNT",StringType).add("L_TAX",StringType).add("L_RETURNFLAG",StringType).add("L_LINESTATUS",StringType).add("L_SHIPDATE",DateType).add("L_COMMITDATE",DateType).add("L_RECEIPTDATE",DateType).add("L_SHIPINSTRUCT",StringType).add("L_SHIPMODE",StringType).add("L_COMMENT",StringType).add("id",IntegerType);

val df = spark.read.schema(schema).option("delimiter", "|").option("escape", "\\").option("quote", "\"").csv("hdfs:///HudiTest/Export_from_MYSQL_lineitem.csv").withColumn("_hoodie_ts",lit(null).cast(TimestampType)).withColumn("_hoodie_date",lit(null).cast(StringType));

df.write.format("org.apache.hudi").option("hoodie.datasource.write.operation","insert").option("hoodie.datasource.write.table.type","COPY_ON_WRITE").option("hoodie.embed.timeline.server",false).option("hoodie.table.name","lineitem_cow").option("hoodie.datasource.write.recordkey.field","id").option("hoodie.datasource.write.partitionpath.field","_hoodie_date").option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://192.168.120.67:10000/test_cdc_hudi").option("hoodie.datasource.hive_sync.database", "test_cdc_hudi").option("hoodie.datasource.hive_sync.username","rapids").option("hoodie.datasource.hive_sync.password","rapids").option("hoodie.datasource.hive_sync.enable",true).option("hoodie.datasource.hive_sync.table","lineitem_cow").option("hoodie.datasource.hive_sync.partition_fields","_hoodie_date").option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor").option("hoodie.datasource.hive_sync.mode","hms").mode(Append).save("/HudiTest/lineitem_cow");

System.exit(0);