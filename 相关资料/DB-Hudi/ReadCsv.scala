
// hudi 0.8 spark 2.4 scala 2.11  ---  CDC 120.66
time spark-shell \
--jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \
--master local[2] \
--driver-class-path $HADOOP_CONF_DIR \
--deploy-mode client \
--driver-memory 2G \
--executor-memory 2G \
--num-executors 2 \
--packages org.apache.spark:spark-avro_2.11:2.4.4 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
-i ./DB-Hudi-append.scala


// hudi 0.9 spark 3.1.2 scala 2.12
time spark-shell \
--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
-i DB-Hudi-demo.scala


// https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark-bundle_2.11/0.9.0/hudi-spark-bundle_2.11-0.9.0.jar



spark-sql \
--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'





///////////////////////////////////////////////////////////////////////////////     Scala Script 


import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
// import org.apache.spark.sql.types.{DataTypes,StructType}
import org.apache.spark.sql.types._

// 定义一个数据结构 加载HDFS 上的CSV 内容

val schema = new StructType().add("id",IntegerType).add("name",StringType).add("gender",StringType).add("city",StringType)


// 创建一个额外的字段 提交时间
val commitTime = System.currentTimeMillis().toString

// 生成 提交时间
val df = spark.read.schema(schema).option("delimiter", ",").option("escape", "\\").option("quote", "\"").csv("/HudiTest/demo.csv").withColumn("ts",lit(commitTime))
// val df = spark.read.schema(schema).option("delimiter", ",").option("escape", "\\").option("quote", "\"").csv("/hudi-test/demo_spark.csv").withColumn("_hoodie_ts",lit(null).cast(TimestampType)).withColumn("_hoodie_date",lit(null).cast(StringType))



// 查看读取的文件
// df.show


// 将 DF 数据写入到 Hudi
df.write.format("org.apache.hudi")

// COPY_ON_WRITE 与 MERGE_ON_READ
.option("hoodie.datasource.write.table.type","MERGE_ON_READ")

// 这是因为我构建hudi 的时候，使用hadoop 3.0.0编译的，
// hadoop3.0.0 提供的是Jetty 9.3; Hudi 依赖于 Jetty 9.4 ( SessionHandler.setHttpOnly() 在9.3版本中并不存在).
// 使用hudi 0.9.0版本内置的hadoop 2.7.3编译则不存在这个问题。
// https://github.com/apache/hudi/issues/3188  https://blog.csdn.net/qq_26502245/article/details/120370826
.option("hoodie.embed.timeline.server",false)

.option("hoodie.table.name","tb_demo")

//  主键 hudi 需要对底层数据进行一个组织：去重、快速定位、检索等等
.option("hoodie.datasource.write.recordkey.field","id")

//  分区 Hudi 存储数据的时候 会按照分区分类存储
.option("hoodie.datasource.write.partitionpath.field","gender")

//  提交时间 将其作为每条数据的提交依据
.option("hoodie.datasource.write.precombine.field","ts")

// 下面为 同步数据 到 hive matestore 的相关参数
.option("hoodie.datasource.hive_sync.enable",true)
.option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://192.168.30.39:10000")
.option("hoodie.datasource.hive_sync.password","hive")
.option("hoodie.datasource.hive_sync.username","hive")
.option("hoodie.datasource.hive_sync.database", "test_db")
.option("hoodie.datasource.hive_sync.table","tb_demo")
// 上面使用的分区键
.option("hoodie.datasource.hive_sync.partition_fields","gender")
// 将分区字段提取为 Hive 分区列的类
.option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor")
// 同步 Hive 必选,同步 hive 的方式 （hms,jdbc,hiveql)
.option("hoodie.datasource.hive_sync.mode","hms")

.mode(Append).save("/HudiTest/hudi_save")




//  表名
//     .option(TBL_NAME.key(),"tb_demo")

//  使用的模式
//     .mode(Overwrite)
//     .mode(Append)

//  hudi 表文件保存路径
//     .save("/HudiTest/demo_Hudi")



// 仅仅 将数据以 hudi 的方式写入 HDFS

df.write.format("org.apache.hudi").option("hoodie.datasource.write.precombine.field","ts").option("hoodie.datasource.write.recordkey.field","id").option("hoodie.datasource.write.partitionpath.field","gender").option("hoodie.table.name","tb_demo").mode(Overwrite).save("/HudiTest/demo_Hudi")


/////////////////////////////////////////////////  sync hudi data in hive matestore
// 会自动在 hive 中创建表

df.write.format("org.apache.hudi").option("hoodie.datasource.write.operation","insert").option("hoodie.datasource.write.table.type","MERGE_ON_READ").option("hoodie.embed.timeline.server",false).option("hoodie.table.name","tb_demo").option("hoodie.datasource.write.recordkey.field","id").option("hoodie.datasource.write.partitionpath.field","gender").option("hoodie.datasource.write.precombine.field","ts").option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://192.168.30.39:10000").option("hoodie.datasource.hive_sync.password","hive").option("hoodie.datasource.hive_sync.username","hive").option("hoodie.datasource.hive_sync.database", "test_db").option("hoodie.datasource.hive_sync.enable",true).option("hoodie.datasource.hive_sync.table","tb_demo").option("hoodie.datasource.hive_sync.partition_fields","gender").option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor").option("hoodie.datasource.hive_sync.mode","hms").mode(Append).save("/HudiTest/hudi_save_mor")








// 使用如下方式 才能查看 MERGE_ON_READ 的表
spark-shell \
--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar \
--driver-class-path $HADOOP_CONF_DIR \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf spark.sql.hive.convertMetastoreParquet=false \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'








// ==============================================================   READ   ============================================================
// hudi 0.9
val resDF = spark.read.format("org.apache.hudi").load("/HudiTest/demo_Hudi")

// hudi 0.8   后面的 /* 是为了加载 分区后的 *.parquet 内容
// val resDF = spark.read.format("org.apache.hudi").load("/HudiTest/demo_Hudi"+"/*")

// 展示数据
resDF.show

// 使用 SparkSQL 的方式查看数据
resDF.createOrReplaceTempView("tb_demo")
spark.sql("select * from tb_demo").show


// 退出
System.exit(0);



// \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ 在 Hive 中创建表
// https://hudi.apache.org/cn/docs/quick-start-guide#create-table

spark.sql("use test_cdc_hudi").show

spark.sql("drop table if exists tb_test").show

// create table if not exists tb_test(id int,name string,gender string,city string) using hudi
// location '/HudiTest/tb_test'
// location 'hdfs://192.168.30.39:9000/HudiTest/tb_test'
// location 'file:///HudiTest/tb_test'
// options (type = 'cow',primaryKey = 'id')

park.sql("desc tb_test").show


