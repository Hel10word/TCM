package com.boraydata.tcm.syncing.hudi;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.utils.FileUtil;

import java.io.File;
import java.util.List;

/** Generate different Scala script according to the configuration file
 * @author bufan
 * @data 2021/12/1
 */
public class ScalaScriptGenerateTool {

    // now support CDC in host 120.66 machine,
    // Hadoop 2.9.0,Zookeeper 3.4.6,Hive 2.3.0,Spark 2.4.4,Scala 2.11,Hudi 0.8
    String sprakShell2 =
            "spark-shell \\\n" +
            "--jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \\\n" +
            "--master local[2] \\\n" +
            "--driver-class-path $HADOOP_CONF_DIR \\\n" +
            "--deploy-mode client \\\n" +
            "--driver-memory 2G \\\n" +
            "--executor-memory 2G \\\n" +
            "--num-executors 2 \\\n" +
            "--packages org.apache.spark:spark-avro_2.11:2.4.4 \\\n" +
            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\\n";

    // Hadoop 3.3.1,Hive 3.1.2,Spark 3.1.2,Scala 2.12.13,Hudi 0.9
    String sprakShell3 =
            "spark-shell \\\n" +
            "--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar  \\\n" +
            "--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \\\n" +
            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\\n";

    StringBuilder scalaScript = new StringBuilder(
//            "import org.apache.hudi.QuickstartUtils._" +
            "import scala.collection.JavaConversions._" +
            "\nimport org.apache.spark.sql.SaveMode._" +
            "\nimport org.apache.hudi.DataSourceReadOptions._" +
            "\nimport org.apache.hudi.DataSourceWriteOptions._" +
            "\nimport org.apache.hudi.config.HoodieWriteConfig._" +
//            "\nimport org.apache.spark.sql.{DataFrame,SparkSession}" +
//            "\nimport org.apache.spark.sql.types.{DataTypes,StructType}\n"
            "\nimport org.apache.spark.sql.types._\n"
    );

    public String initSriptFile(Table table,String hdfsSourceFilePath, DatabaseConfig dbConfig, TableCloneManageConfig tcmConfig){
        String delimiter = tcmConfig.getDelimiter();
        String hoodieTableType = tcmConfig.getHoodieTableType();
        String primaryKey = tcmConfig.getPrimaryKey();
        String partitionKey = tcmConfig.getPartitionKey();
        String hdfsCloneDataPath = tcmConfig.getHdfsCloneDataPath();

        this
                .setSchema(table).setDF(delimiter,hdfsSourceFilePath)
                .setWrite(hoodieTableType,table.getTablename(),primaryKey,partitionKey,dbConfig.getUrl(),dbConfig.getDatabasename(),dbConfig.getUsername(),dbConfig.getPassword(),hdfsCloneDataPath)
                .setScalaEnd();
        return this.scalaScript.toString();
    }

    public String loadCommand(String hdfsSourceDataDir,String localCsvPath,String hdfsCloneDataPath,String scriptPath){
        StringBuilder loadShell = new StringBuilder();
        loadShell
                .append("hdfs dfs -mkdir -p ").append(hdfsSourceDataDir)
                .append("\nhdfs dfs -rm -r ").append(hdfsCloneDataPath)
                .append("\ntime hdfs dfs -put -f ").append(localCsvPath).append(" ").append(hdfsSourceDataDir)
                .append("\n\n").append("time ").append(sprakShell2)
                .append("-i ").append(scriptPath);
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
    private ScalaScriptGenerateTool setSchema(Table table){
        this.scalaScript.append("\nval schema = new StructType()");
        List<Column> columns = table.getColumns();
        if(columns == null || columns.size() == 0)
            return this;
        for ( Column column : columns){
            String columnName = column.getColumnName();
            TableCloneManageType tcmMappingType = column.getTableCloneManageType();
            String outDataType = tcmMappingType.getOutDataType(DataSourceType.HUDI);
            this.scalaScript.append(".add(\"").append(columnName).append("\",").append(outDataType).append(")");
        }
        this.scalaScript.append(";\n");
        return this;
    }

//        val commitTime = System.currentTimeMillis().toString
    private ScalaScriptGenerateTool setTime(){
        this.scalaScript.append("\nval commitTime = System.currentTimeMillis().toString;\n");
        return this;
    }

    //    val df = spark.read.schema(schema).option("delimiter", ",").option("escape", "\\").option("quote", "\"")
    //    .csv("/hudi-test/demo_spark.csv")
    //    .withColumn("_hoodie_ts",lit(null).cast(TimestampType))
    //    .withColumn("_hoodie_date",lit(null).cast(StringType));
    private ScalaScriptGenerateTool setDF(String delimiter,String hdfsCsvPath){
        String options = ".option(\"delimiter\", \""+delimiter+"\").option(\"escape\", \"\\\\\").option(\"quote\", \"\\\"\")";
        this.scalaScript.append("\nval df = spark.read.schema(schema)").append(options)
                .append(".csv(\"").append(hdfsCsvPath).append("\")")
//        https://stackoverflow.com/questions/57945174/how-to-convert-timestamp-to-bigint-in-a-pyspark-dataframe
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
    private ScalaScriptGenerateTool setWrite(String hoodieTableType,String tableName,String primaryKey,String partitionKey,String jdbcUrl,String database,String user,String pwd,String hdfsCloneDataPath){
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


    private ScalaScriptGenerateTool setScalaEnd(){
        this.scalaScript.append("\nSystem.exit(0);");
        return this;
    }

}
