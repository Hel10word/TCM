package com.boraydata.cdc.tcm.syncing.util;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.syncing.DataSyncingCSVConfigTool;
import com.boraydata.cdc.tcm.utils.StringUtil;

import java.util.List;
import java.util.Objects;


/**
 * Scala DataType for Hudi
 *  https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/types/
 *  https://www.scala-lang.org/api/2.13.4/scala/index.html
 *  https://www.tutorialspoint.com/scala/scala_data_types.htm
 *
 * scala> org.apache.spark.sql.types.
 * AbstractDataType   BinaryType             CharType    Decimal       HIVE_TYPE_STRING   MapType           NumericType             ShortType     TimestampType
 * AnyDataType        BooleanType            DataType    DecimalType   HiveStringType     Metadata          ObjectType              StringType    UDTRegistration
 * ArrayType          ByteType               DataTypes   DoubleType    IntegerType        MetadataBuilder   PythonUserDefinedType   StructField   VarcharType
 * AtomicType         CalendarIntervalType   DateType    FloatType     LongType           NullType          SQLUserDefinedType      StructType    package
 *
 * DecimalType(M,D) M <= 38,D <= M <= 38 default (10,0)
 * |           SType|      int|
 * |           ShortType|      int|
 * |         IntegerType|      int|
 * |            LongType|   bigint|
 * |           FloatType|    float|
 * |          DoubleType|   double|
 * |         BooleanType|  boolean|
 * |          BinaryType|   binary|
 * |            DateType|     date|
 * |       TimestampType|   bigint|
 * |              gender|   string|
 *
 * TimestampType 类型的数据会被转为 Bigint 类型来存储
 * https://stackoverflow.com/questions/57945174/how-to-convert-timestamp-to-bigint-in-a-pyspark-dataframe
 *
 */

/** Generate different Scala script according to the common file
 * @author bufan
 * @date 2021/12/1
 *
 * https://hudi.apache.org/docs/0.9.0/writing_data
 */
public class ScalaScriptGenerateUtil {

    // now support CDC in host 120.66 machine,
    // Hadoop 2.9.0,Zookeeper 3.4.6,Hive 2.3.0,Spark 2.4.4,Scala 2.11,Hudi 0.8
//    String sprakShell2 =
//            "spark-shell \\\n" +
////            "--jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \\\n" +
////            "--master local[2] \\\n" +
//            "--driver-class-path $HADOOP_CONF_DIR \\\n" +
////            "--deploy-mode client \\\n" +
////            "--driver-memory 2G \\\n" +
////            "--executor-memory 2G \\\n" +
////            "--num-executors 2 \\\n" +
//            "--packages org.apache.spark:spark-avro_2.11:2.4.4 \\\n" +
//            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\\n";

    // Hadoop 3.3.1,Hive 3.1.2,Spark 3.1.2,Scala 2.12.13,Hudi 0.9
//    String sprakShell3 =
//            "spark-shell \\\n" +
////            "--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar  \\\n" +
////            "--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \\\n" +
//            "--packages org.apache.spark:spark-avro_2.12:3.0.1 \\\n" +
//            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\\n";

    StringBuilder scalaScript = new StringBuilder(
//            "import org.apache.hudi.QuickstartUtils._" +
            "import scala.collection.JavaConversions._" +
            "\nimport org.apache.spark.sql.SaveMode._" +
            "\nimport org.apache.hudi.DataSourceReadOptions._" +
            "\nimport org.apache.hudi.DataSourceWriteOptions._" +
            "\nimport org.apache.hudi.config.HoodieWriteConfig._" +
//            "\nimport org.apache.spark.sql.{DataFrame,SparkSession}" +
//            "\nimport org.apache.spark.sql.types.{DataTypes,StructType}\n"
            "\nimport org.apache.spark.sql.types._" +
            "\ntry{\n"
    );

    public String initSriptFile(Table table, String hdfsSourceFilePath, DatabaseConfig dbConfig, TableCloneManagerConfig tcmConfig){
        String delimiter = tcmConfig.getDelimiter();
        String lineSeparate = tcmConfig.getLineSeparate();
        String quote = tcmConfig.getQuote();
        String escape = tcmConfig.getEscape();
        String hoodieTableType = tcmConfig.getHoodieTableType();
        String primaryKey = tcmConfig.getPrimaryKey();
        String partitionKey = tcmConfig.getPartitionKey();
        String hdfsCloneDataPath = tcmConfig.getHdfsCloneDataPath();
        String tableName = table.getTableName();
        String databaseName = dbConfig.getDatabaseName();

        this
                .setDatabase(databaseName)
                .setDropOldTable(tableName,hoodieTableType)
                .setSchema(table)
                .setDF(delimiter,lineSeparate,quote,escape,hdfsSourceFilePath)
                .setWrite(hoodieTableType,tableName,primaryKey,partitionKey, DatasourceConnectionFactory.getJDBCUrl(dbConfig),databaseName,dbConfig.getUsername(),dbConfig.getPassword(),hdfsCloneDataPath)
                .setRefreshTable(tableName,hoodieTableType)
                .setScalaEnd();
        return this.scalaScript.toString();
    }

    private ScalaScriptGenerateUtil setRefreshTable(String tableName,String hoodieTableType) {
        if("MERGE_ON_READ".equals(hoodieTableType)) {
            this.scalaScript.append("\nspark.sql(\"REFRESH TABLE ").append(tableName).append("_ro\");");
            this.scalaScript.append("\nspark.sql(\"REFRESH TABLE ").append(tableName).append("_rt\");\n");
        }else
            this.scalaScript.append("\nspark.sql(\"REFRESH TABLE ").append(tableName).append("\");\n");
        return this;
    }

    private ScalaScriptGenerateUtil setDropOldTable(String tableName,String hoodieTableType) {
        if("MERGE_ON_READ".equals(hoodieTableType)) {
            this.scalaScript.append("\nspark.sql(\"DROP TABLE IF EXISTS ").append(tableName).append("_ro\");");
            this.scalaScript.append("\nspark.sql(\"DROP TABLE IF EXISTS ").append(tableName).append("_rt\");\n");
        }else
            this.scalaScript.append("\nspark.sql(\"DROP TABLE IF EXISTS ").append(tableName).append("\");\n");
        return this;
    }

    private ScalaScriptGenerateUtil setDatabase(String databaseName) {
        this.scalaScript.append("\nspark.sql(\"use ").append(databaseName).append("\");\n");

        return this;
    }

    public String loadCommand(String hdfsSourceDataDir, String localCsvPath, String hdfsCloneDataPath, String scriptPath, String sparkStartCommand, Boolean csvSaveInHDFS){
        StringBuilder loadShell = new StringBuilder();

        loadShell
                .append("\nhdfs dfs -rm -r ").append(hdfsCloneDataPath)
                .append("\nhdfs dfs -mkdir -p ").append(hdfsCloneDataPath);
        if(Boolean.TRUE.equals(csvSaveInHDFS)){
            loadShell
                    .append("\nhdfs dfs -mkdir -p ").append(hdfsSourceDataDir)
                    .append("\ntime hdfs dfs -put -f ").append(localCsvPath).append(" ").append(hdfsSourceDataDir).append(" 2>&1")
                    // if the CSV File large and disk no space,save part of CSV for execute Spark-Shell
                    .append("\nhead -5 ").append(localCsvPath).append(" > ").append(localCsvPath).append("_sample_data")
                    .append("\nrm -f ").append(localCsvPath)
                    ;
        }
        loadShell.append("\n\n").append("time ").append(sparkStartCommand).append(" -i ").append(scriptPath).append(" > ").append(scriptPath).append(".out");
        if(Boolean.TRUE.equals(csvSaveInHDFS)){
            loadShell.append("\nhdfs dfs -rm -f ").append(hdfsSourceDataDir).append(localCsvPath);
        }
        return loadShell.toString();
    }

    public String getScript(){
        return this.scalaScript.toString();
    }

    //    val schema = new StructType()
    //    .add("id",StringType)
    //    .add("name",StringType)
    //    .add("age",StringType)
    //    .add("gender",StringType)
    //    .add("city",StringType)
    //    ;
    private ScalaScriptGenerateUtil setSchema(Table table){
        this.scalaScript.append("\nval schema = new StructType()");
        List<Column> columns = table.getColumns();
        if(columns == null || columns.size() == 0)
            return this;
        for ( Column column : columns){
            String columnName = column.getColumnName();
            TCMDataTypeEnum tcmMappingType = column.getTcmDataTypeEnum();
            String outDataType = tcmMappingType.getMappingDataType(DataSourceEnum.HUDI);
            if(TCMDataTypeEnum.DECIMAL.equals(tcmMappingType)){
                outDataType = outDataType+"(";
                Integer precision = column.getNumericPrecision();
                Integer scale = column.getNumericScale();
                if(Objects.isNull(precision))
                    outDataType = outDataType+"0,";
                else
                    outDataType = outDataType+precision+",";
                if(Objects.isNull(scale))
                    outDataType = outDataType+"0)";
                else
                    outDataType = outDataType+scale+")";
            }
            this.scalaScript.append(".add(\"").append(columnName).append("\",").append(outDataType).append(")");
        }
        this.scalaScript.append(";\n");
        return this;
    }

//        val commitTime = System.currentTimeMillis().toString
    private ScalaScriptGenerateUtil setTime(){
        this.scalaScript.append("\nval commitTime = System.currentTimeMillis().toString;\n");
        return this;
    }

    /**
     * @see <a href="https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html"></a>
     */
    //    val df = spark.read.schema(schema).option("delimiter", ",").option("escape", "\\").option("quote", "\"")
    //    .csv("/hudi-test/demo_spark.csv")
    //    .withColumn("_hoodie_ts",lit(null).cast(TimestampType))
    //    .withColumn("_hoodie_date",lit(null).cast(StringType));
    private ScalaScriptGenerateUtil setDF(String delimiter,String lineSeparate,String quote,String escape, String hdfsCsvPath){
//        String options = ".option(\"delimiter\", \""+delimiter+"\").option(\"escape\", \"\\\\\").option(\"quote\", \"\\\"\")";
//        this.scalaScript.append("\nval df = spark.read.schema(schema)").append(options)
//                .append(".csv(\"").append(hdfsCsvPath).append("\")")
        this.scalaScript.append("\nval df = spark.read.schema(schema)");
        delimiter = StringUtil.escapeRegexDoubleQuoteEncode(delimiter);
        quote = StringUtil.escapeRegexDoubleQuoteEncode(quote);
        escape = StringUtil.escapeRegexDoubleQuoteEncode(escape).replaceAll("\\\\","\\\\\\\\");
        if(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7.equals(delimiter))
            this.scalaScript.append(".option(\"delimiter\",\"\\u0007\")");
        else
            this.scalaScript.append(".option(\"delimiter\",\"").append(delimiter).append("\")");

        if(StringUtil.nonEmpty(quote))
            this.scalaScript.append(".option(\"quote\",\"").append(quote).append("\")");
        if(StringUtil.nonEmpty(escape))
            this.scalaScript.append(".option(\"escape\",\"").append(escape).append("\")");
//        this.scalaScript.append(".csv(\"file://").append(hdfsCsvPath).append("\")")
        this.scalaScript.append(".csv(\"").append(hdfsCsvPath).append("\")")
                /**
                 * @see <a href="https://stackoverflow.com/questions/32067467/create-new-dataframe-with-empty-null-field-values"></a>
                 * @see <a href="https://stackoverflow.com/questions/57945174/how-to-convert-timestamp-to-bigint-in-a-pyspark-dataframe"></a>
                 * @since 芷琪 说建表时需要增加两个字段进去，导入的数据可以为空 （__hoodie_ts TimestampType,_hoodie_date StringType）
                 * Select * from table
                 *  _hoodie_ts => NULL
                 *  _hoodie_date => default
                 */
                .append(".withColumn(\"_hoodie_ts\",lit(null).cast(TimestampType))")
                .append(".withColumn(\"_hoodie_date\",lit(null).cast(StringType));\n");
        return this;
    }

    //     df.write.format("org.apache.hudi")
    //     .option("hoodie.datasource.write.operation","insert")
    //     .option("hoodie.datasource.write.table.type","COPY_ON_WRITE")
    //     .option("hoodie.embed.timeline.server",false)
    //     .option("hoodie.table.name","tb_demo_cow")
    //     .option("hoodie.datasource.write.recordkey.field","id")
    //     .option("hoodie.datasource.write.partitionpath.field","gender")
    //     ---------.option("hoodie.datasource.write.precombine.field","_hoodie_date")
    //     .option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://192.168.120.67:10000")
    //     .option("hoodie.datasource.hive_sync.database", "test_cdc_hudi")
    //     .option("hoodie.datasource.hive_sync.username","rapids")
    //     .option("hoodie.datasource.hive_sync.password","rapids")
    //     .option("hoodie.datasource.hive_sync.enable",true)
    //     .option("hoodie.datasource.hive_sync.table","tb_demo_cow")
    //     .option("hoodie.datasource.hive_sync.partition_fields","gender")
    //     .option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor")
    //     .option("hoodie.datasource.hive_sync.mode","hms")
    //     .mode(Append).save("/HudiTest/hudi_save_test_cop");
    private ScalaScriptGenerateUtil setWrite(String hoodieTableType, String tableName, String primaryKey, String partitionKey, String jdbcUrl, String database, String user, String pwd, String hdfsCloneDataPath){
        this.scalaScript.append("\ndf.write.format(\"org.apache.hudi\")")
                .append(String.format(".option(\"hoodie.datasource.write.operation\",\"%s\")","insert"))
                .append(String.format(".option(\"hoodie.datasource.write.table.type\",\"%s\")",hoodieTableType))
                .append(String.format(".option(\"hoodie.embed.timeline.server\",%s)","false"))
                .append(String.format(".option(\"hoodie.table.name\",\"%s\")",tableName))
                .append(String.format(".option(\"hoodie.datasource.write.recordkey.field\",\"%s\")",primaryKey))
                .append(String.format(".option(\"hoodie.datasource.write.partitionpath.field\",\"%s\")",partitionKey))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.jdbcurl\", \"%s\")",jdbcUrl))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.database\", \"%s\")",database))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.username\",\"%s\")",user))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.password\",\"%s\")",pwd))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.enable\",%s)","true"))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.table\",\"%s\")",tableName))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.partition_fields\",\"%s\")",partitionKey))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.partition_extractor_class\",\"%s\")","org.apache.hudi.hive.MultiPartKeysValueExtractor"))
                .append(String.format(".option(\"hoodie.datasource.hive_sync.mode\",\"%s\")","hms"))
                .append(String.format(".mode(%s).save(\"%s\");","Append",hdfsCloneDataPath)).append("\n");


        return this;
    }


    private ScalaScriptGenerateUtil setScalaEnd(){
        this.scalaScript
                .append("}\ncatch {" +
                        "\ncase ex: Exception => println(ex.getMessage)" +
                        "\n}")
                .append("\nfinally {" +
                        "\nSystem.exit(0);" +
                        "\n}");
        return this;
    }

}
