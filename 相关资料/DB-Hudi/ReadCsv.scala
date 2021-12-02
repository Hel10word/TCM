

spark-shell \
  --jars ./hudi-spark3-bundle_2.12-0.9.0.jar \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'


// hudi 0.8 spark 2.4 scala 2.11
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




import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.types.{DataTypes,StructType}

// 定义一个数据结构 加载HDFS 上的CSV 内容
// val schema = new StructType()
//     .add("id",DataTypes.StringType)......

val schema = new StructType().add("id",DataTypes.StringType).add("name",DataTypes.StringType).add("age",DataTypes.StringType).add("gender",DataTypes.StringType).add("city",DataTypes.StringType)


// 创建一个额外的字段 提交时间
val commitTime = System.currentTimeMillis().toString

// 生成 提交时间
val df = spark.read.schema(schema).csv("/HudiTest/demo.csv").withColumn("ts",lit(commitTime))


// 查看读取的文件
// df.show


// 将 DF 数据写入到 Hudi
// df.write.format("hudi")
//     .options(getQuickstartWriteConfigs)

//  提交时间 将其作为每条数据的提交依据
//     .option(PRECOMBINE_FIELD.key(),"ts")

//  主键 hudi 需要对底层数据进行一个组织：去重、快速定位、检索等等
//     .option(RECORDKEY_FIELD.key(),"id")

//  分区 类似与 Hive 的分区
//     .option(PARTITIONPATH_FIELD.key(),"gender")

//  表名
//     .option(TBL_NAME.key(),"tb_demo")

//  使用的模式
//     .mode(Overwrite)

//  hudi 表文件保存路径
//     .save("/HudiTest/demo_Hudi")


// hudi 0.9
df.write.format("hudi").options(getQuickstartWriteConfigs).option(PRECOMBINE_FIELD.key(),"ts").option(RECORDKEY_FIELD.key(),"id").option(PARTITIONPATH_FIELD.key(),"gender").option(TBL_NAME.key(),"tb_demo").mode(Overwrite).save("/HudiTest/demo_Hudi")
// 这是因为我构建hudi 的时候，使用hadoop 3.0.0编译的，
// hadoop3.0.0 提供的是Jetty 9.3; Hudi 依赖于 Jetty 9.4 ( SessionHandler.setHttpOnly() 在9.3版本中并不存在).
// 使用hudi 0.9.0版本内置的hadoop 2.7.3编译则不存在这个问题。
// https://github.com/apache/hudi/issues/3188  https://blog.csdn.net/qq_26502245/article/details/120370826
// df.write.format("hudi").options(getQuickstartWriteConfigs).option(PRECOMBINE_FIELD.key(),"ts").option(RECORDKEY_FIELD.key(),"id").option(PARTITIONPATH_FIELD.key(),"gender").option(TBL_NAME.key(),"tb_demo").option("hoodie.embed.timeline.server","false").mode(Overwrite).save("/HudiTest/demo_Hudi")


// hudi 0.8
// df.write.format("hudi").options(getQuickstartWriteConfigs).option(PRECOMBINE_FIELD_OPT_KEY,"ts").option(RECORDKEY_FIELD_OPT_KEY,"id").option(PARTITIONPATH_FIELD_OPT_KEY,"gender").option(TABLE_NAME,"table_demo1").mode(Overwrite).save("/HudiTest/demo_Hudi")
// df.write.format("hudi").options(getQuickstartWriteConfigs).option(PRECOMBINE_FIELD_OPT_KEY,"ts").option(RECORDKEY_FIELD_OPT_KEY,"id").option(PARTITIONPATH_FIELD_OPT_KEY,"gender").option(TABLE_NAME,"table_demo1").mode(Append).save("/HudiTest/demo_Hudi")



// ==============================================================   READ   ============================================================
// hudi 0.9
val resDF = spark.read.format("hudi").load("/HudiTest/demo_Hudi")

// hudi 0.8
// val resDF = spark.read.format("org.apache.hudi").load("/HudiTest/demo_Hudi"+"/*")

// 展示数据
// resDF.show

// 使用 SparkSQL 的方式查看数据
resDF.createOrReplaceTempView("tb_demo01")
spark.sql("select * from tb_demo01").show



// 退出
System.exit(0);