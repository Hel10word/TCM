//package com.boraydata.tcm.db2hive;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.fastjson.serializer.SerializerFeature;
//import org.apache.spark.launcher.SparkLauncher;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//import java.util.ArrayList;
//import java.util.Enumeration;
//import java.util.List;
//import java.util.Properties;
//import java.util.stream.Stream;
//
///**
// * TODO
// *
// * @date: 2021/1/13
// * @author: hatter
// **/
//public class HudiBulkInsert {
//
//    private String avroPath;
//    private String configPath;
//    private Parameter parameter;
//
//    private static final Logger log = LoggerFactory.getLogger(HudiBulkInsert.class);
//
//    public HudiBulkInsert(Parameter parameter) {
//        this.parameter = parameter;
//    }
//
//    public void avroGenerator() throws IOException {
//        avroPath = parameter.cdcHome + "/init/" + parameter.pgDatabase + "_" + parameter.pgTable.replace('.', '_') + "_data.avsc";
//
//        Path path = Paths.get(avroPath);
//        if (!Files.exists(path)) {
//            String avroStr = new DB2Avro().getAvroSchema(parameter.pgConnUrl, parameter.pgUser, parameter.pgPassword, parameter.pgTable);
//            JSONObject object = JSONObject.parseObject(avroStr);
//            avroStr = JSON.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
//                    SerializerFeature.WriteDateUseDateFormat);
//            Files.write(path, avroStr.getBytes(), StandardOpenOption.CREATE_NEW);
//        }
//    }
//
//    public void configGenerator() throws IOException {
//        Properties prop = new Properties();
//        prop.setProperty("hoodie.bulkinsert.shuffle.parallelism", parameter.parallel);
//
//        prop.setProperty("hoodie.datasource.write.recordkey.field", parameter.primaryKey);
//        prop.setProperty("hoodie.datasource.write.partitionpath.field", parameter.partitionKey);
//
//        prop.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", "file://" + avroPath);
//        prop.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", "file://" + avroPath);
//
//        prop.setProperty("hoodie.deltastreamer.source.dfs.root", parameter.csvPath);
//
//        prop.setProperty("hoodie.datasource.hive_sync.support_timestamp", "true");
//
//        prop.setProperty("hoodie.datasource.write.keygenerator.class", parameter.nonPartitioned ?
//                "org.apache.hudi.keygen.NonpartitionedKeyGenerator" : (parameter.primaryKey.split(",").length > 1 ?
//                "org.apache.hudi.keygen.ComplexKeyGenerator" : "org.apache.hudi.keygen.SimpleKeyGenerator"));
//
//        configPath = parameter.cdcHome + "/init/" + parameter.pgDatabase + "_" + parameter.pgTable.replace('.', '_') + "_source.properties";
//
//        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configPath), StandardCharsets.UTF_8));
//        bw.newLine();
//        for (Enumeration<?> e = prop.keys(); e.hasMoreElements(); ) {
//            String key = (String) e.nextElement();
//            String val = prop.getProperty(key);
//            bw.write(key + "=" + val);
//            bw.newLine();
//        }
//        bw.flush();
//        bw.close();
//    }
//
//    public void insertHDFS() throws IOException, InterruptedException {
//        SparkLauncher spark = new SparkLauncher()
//                .setVerbose(true)
//                .setAppResource(parameter.cdcHome + "/init/hudi-utilities-bundle_2.11-0.8.0.jar");
//
////        /**
////         spark.executor.cores=4
////         spark.driver.memory=8g
////         spark.driver.cores=4
////         */
////        spark.setConf("spark.driver.cores", "4");
////        spark.setConf("spark.executor.cores", "6");
////        spark.setConf("spark.driver.memory", "16g");
////        spark.setConf("spark.executor.memory", "24g");
//
//        spark.setMainClass("org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer").addAppArgs(getInsertArgs());
//
//        runSparkProcess(spark, "Write HDFS");
//    }
//
//    private String[] getInsertArgs() {
//        List<String> configList = new ArrayList<>();
//        Config configs = new Config(configList);
//        configs.setConfigs("--props", "file://" + configPath);
//        configs.setConfigs("--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider");
//        configs.setConfigs("--source-class", "org.apache.hudi.utilities.sources.CsvDFSSource");
//        configs.setConfigs("--source-ordering-field", parameter.primaryKey.split(",")[0]);
//        configs.setConfigs("--target-base-path", parameter.hdfsPath);
//        configs.setConfigs("--target-table", parameter.hiveTable);
//        configs.setConfigs("--op", "BULK_INSERT");
//        configs.setConfigs("--table-type", parameter.hoodieTableType);
//        configs.setConfigs("--disable-compaction");
//
//        configList.forEach(c -> System.out.println(c + " "));
//        return configList.toArray(new String[0]);
//    }
//
//    public void syncHive()
//            throws IOException, InterruptedException {
//        SparkLauncher spark = new SparkLauncher()
//                .setVerbose(true)
//                .setAppResource(parameter.cdcHome + "/sync/hivesync-tool-1.0.jar");
//
//        String lib = parameter.cdcHome + "/sync/lib";
//        File[] values = new File(lib).listFiles();
//        if (values != null) {
//            Stream.of(values).map(File::getAbsolutePath).forEach(spark::addJar);
//        }
//
//        spark.setMainClass("com.boray.stream.hivesync.HiveSyncTool").addAppArgs(getSynctArgs());
//
//        runSparkProcess(spark, "Hive sync");
//    }
//
//    private String[] getSynctArgs() {
//        List<String> configList = new ArrayList<>();
//        Config configs = new Config(configList);
//        if (parameter.hiveUser != null)
//            configs.setConfigs("-u", parameter.hiveUser);
//        if (parameter.hivePassword != null)
//            configs.setConfigs("-pw", parameter.hivePassword);
//        if (!parameter.nonPartitioned)
//            configs.setConfigs("-pt", parameter.partitionKey);
//        if (parameter.hiveMultiPartitionKeys)
//            configs.setConfigs("-mt");
//
//        // default config
//        configs.setConfigs("-bp", parameter.hdfsPath);
//        configs.setConfigs("-ju", parameter.hiveUrl);
//        configs.setConfigs("-dn", parameter.hiveDatabase);
//        configs.setConfigs("-tn", parameter.hiveTable);
//        return configList.toArray(new String[0]);
//    }
//
//    private void runSparkProcess(SparkLauncher spark, String str) throws IOException, InterruptedException {
//        Process proc = spark.launch();
//
//        SparkInputStreamReader inputStreamReaderRunnable = new SparkInputStreamReader(proc.getInputStream(), "input");
//        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
//        inputThread.start();
//
//        SparkInputStreamReader errorStreamReaderRunnable = new SparkInputStreamReader(proc.getErrorStream(), "error");
//        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
//        errorThread.start();
//
//        System.out.println("Waiting for finish...");
//        int exitCode = proc.waitFor();
//        System.out.println("Finished! " + str + (exitCode == 0 ? " succeed" : " failure"));
//    }
//}