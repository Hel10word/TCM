package com.boraydata.tcm.syncing.hudi;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.FileUtil;

import java.io.File;
import java.util.List;

/** Generate different Scala script according to the configuration file
 * @author bufan
 * @data 2021/12/1
 */
public class ScalaScriptGenerateTool {

    // now support CDC in host 120.66 machine,
    // Hadoop 2.9.0,Zookeeper 3.4.6,Hive 2.3.0,Spark 2.4.4,
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

    String sprakShell3 =
            "spark-shell \\\n" +
            "--jars ./hudi-spark3-bundle_2.12-0.9.0.jar \\\n" +
            "--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \\\n" +
            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \\\n";

    StringBuilder scalaScript = new StringBuilder(
            "import org.apache.hudi.QuickstartUtils._" +
            "\nimport scala.collection.JavaConversions._" +
            "\nimport org.apache.spark.sql.SaveMode._" +
            "\nimport org.apache.hudi.DataSourceReadOptions._" +
            "\nimport org.apache.hudi.DataSourceWriteOptions._" +
            "\nimport org.apache.hudi.config.HoodieWriteConfig._" +
//            "\nimport org.apache.spark.sql.{DataFrame,SparkSession}" +
            "\nimport org.apache.spark.sql.types.{DataTypes,StructType}\n"
    );

    public boolean initSriptFile(AttachConfig atCfg){
        this.setSchema(atCfg.getColNames())
                .setTime()
                .setDF(atCfg.getHdfsCsvPath(),"ts")
                .setScalaSpark3("ts",atCfg.getHudiPrimaryKey(),atCfg.getHiveNonPartitioned(),atCfg.getHudiPartitionKey(),atCfg.getCloneTableName(),atCfg.getHudiHdfsPath())
        ;
        String configPath = new File(atCfg.getTempDirectory(),atCfg.getSourceDataType() + "_to_hudi.scala").getPath();
        atCfg.setLoadExpendScriptPath(configPath);
        return FileUtil.WriteMsgToFile(this.getScript(),configPath);
    }
    public String loadCommand(AttachConfig atCfg){
        StringBuilder loadShell = new StringBuilder();
        String localCsvPath = atCfg.getLocalCsvPath();
//        if(!FileUtil.Exists(localCsvPath))
//            throw new TCMException("unable put CSV file to HDFS,check the CSV file is exists in '"+localCsvPath+'\'');
        loadShell
                .append("hdfs dfs -rm ").append(atCfg.getHudiHdfsPath())
                .append("\nhdfs dfs -rm ").append(atCfg.getHdfsCsvPath())
                .append("\ntime hdfs dfs -put ").append(localCsvPath).append(" ").append(atCfg.getHdfsCsvDir())
                .append("\n\n").append("time ").append(sprakShell2)
                .append("-i ").append(atCfg.getLoadExpendScriptPath()).append(" 2>&1");
        return loadShell.toString();
    }

    public String getScript(){
        return this.scalaScript.toString();
    }

    //    val schema = new StructType().add("id",DataTypes.StringType).add("name",DataTypes.StringType).add("age",DataTypes.StringType).add("gender",DataTypes.StringType).add("city",DataTypes.StringType)
    private ScalaScriptGenerateTool setSchema(String[] ColNames){
        this.scalaScript.append("\nval schema = new StructType()");
        if(ColNames == null || ColNames.length == 0)
            return this;
        for (String item : ColNames)
            this.scalaScript.append(".add(\"").append(item).append("\",DataTypes.StringType)");
        this.scalaScript.append("\n");
        return this;
    }

//        val commitTime = System.currentTimeMillis().toString
    private ScalaScriptGenerateTool setTime(){
        this.scalaScript.append("\nval commitTime = System.currentTimeMillis().toString\n");
        return this;
    }

    //    val df = spark.read.schema(schema).csv("/test/demo.csv").withColumn("ts",lit(commitTime))
    private ScalaScriptGenerateTool setDF(String csvPath,String commitName){
        this.scalaScript.append("\nval df = spark.read.schema(schema).csv(\"").append(csvPath).append("\").withColumn(\"").append(commitName).append("\",lit(commitTime))\n");
        return this;
    }

    private ScalaScriptGenerateTool setScalaSpark2(String commitKey,String recordKey,Boolean partFlag,String partKey,String tableName,String savePath){
        this.scalaScript
                .append("\ndf.write.format(\"hudi\").options(getQuickstartWriteConfigs)")
                .append(".option(PRECOMBINE_FIELD_OPT_KEY,\"").append(commitKey).append("\")")
                .append(".option(RECORDKEY_FIELD_OPT_KEY,\"").append(recordKey).append("\")");
        if(Boolean.TRUE.equals(partFlag))
            this.scalaScript.append(".option(PARTITIONPATH_FIELD_OPT_KEY,\"").append(partKey).append("\")");
        this.scalaScript
                .append(".option(TABLE_NAME,\"").append(tableName).append("\")")
                .append(".save(\"").append(savePath).append("\")\n");
        return this;
    }
    private ScalaScriptGenerateTool setScalaSpark3(String commitKey,String recordKey,Boolean partFlag,String partKey,String tableName,String savePath){
        this.scalaScript
                .append("df.write.format(\"hudi\").options(getQuickstartWriteConfigs)")
                .append(".option(PRECOMBINE_FIELD.key(),\"").append(commitKey).append("\")")
                .append(".option(RECORDKEY_FIELD.key(),\"").append(recordKey).append("\")");
        if(Boolean.TRUE.equals(partFlag))
            this.scalaScript.append(".option(PARTITIONPATH_FIELD.key(),\"").append(partKey).append("\")");
        this.scalaScript
                .append(".option(TBL_NAME.key(),\"").append(tableName).append("\")")
                .append(".mode(Overwrite)")
                .append(".save(\"").append(savePath).append("\")");
        return this;
    }



}
