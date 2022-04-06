package com.boraydata.tcm;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.*;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * @author bufan
 * @data 2021/11/5
 */
public class TestDataProvider {

    //========================== MySQL ===============================
    public static DatabaseConfig.Builder builderMySQL = new DatabaseConfig.Builder();
    public static DatabaseConfig configMySQL = builderMySQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.38")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    //========================== PgSQL ===============================
    public static DatabaseConfig.Builder builderPGSQL = new DatabaseConfig.Builder();
    public static DatabaseConfig configPGSQL = builderPGSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.31")
            .setPort("5432")
            .setUsername("root")
            .setPassword("root")
            .create();

    //========================== Hudi ===============================
    public static DatabaseConfig.Builder builderHudi = new DatabaseConfig.Builder();
    public static DatabaseConfig configHudi = builderHudi
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.HUDI)
            .setHost("192.168.120.67")
            .setPort("10000")
            .setUsername("rapids")
            .setPassword("rapids")
            .create();


    public static String sparkCustomCommand = "spark-shell \\\n" +
            "            --jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \\\n" +
            "            --driver-class-path $HADOOP_CONF_DIR \\\n" +
            "            --packages org.apache.spark:spark-avro_2.11:2.4.4 \\\n" +
            "            --master local[2] \\\n" +
            "            --deploy-mode client \\\n" +
            "            --driver-memory 2G \\\n" +
            "            --executor-memory 2G \\\n" +
            "            --num-executors 2";

    public static Properties getProperties(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        Properties properties = new Properties();
        properties.setProperty("sourceDatabaseName",sourceConfig.getDatabasename());
        properties.setProperty("sourceDataType",sourceConfig.getDataSourceType().toString());
        properties.setProperty("sourceHost",sourceConfig.getHost());
        properties.setProperty("sourcePort",sourceConfig.getPort());
        properties.setProperty("sourceUser",sourceConfig.getUsername());
        properties.setProperty("sourcePassword",sourceConfig.getPassword());
        properties.setProperty("sourceTable",sourceConfig.getTableName()==null?"":sourceConfig.getTableName());

        properties.setProperty("cloneDatabaseName",cloneConfig.getDatabasename());
        properties.setProperty("cloneDataType",cloneConfig.getDataSourceType().toString());
        properties.setProperty("cloneHost",cloneConfig.getHost());
        properties.setProperty("clonePort",cloneConfig.getPort());
        properties.setProperty("cloneUser",cloneConfig.getUsername());
        properties.setProperty("clonePassword",cloneConfig.getPassword());
        properties.setProperty("cloneTable",cloneConfig.getTableName()==null?"":cloneConfig.getTableName());

        properties.setProperty("hdfsSourceDataPath","hdfs://192.168.30.39:9000/HudiTest/");
//        properties.setProperty("hdfsSourceDataPath","file:///HudiTest/");
//        properties.setProperty("hdfsSourceDataPath","/HudiTest/");
        properties.setProperty("hdfsCloneDataPath","/HudiTest/");
        properties.setProperty("primaryKey","id");
        properties.setProperty("partitionKey","_hoodie_date");
        properties.setProperty("hudiTableType","MERGE_ON_READ");
//        properties.setProperty("hudiTableType","COPY_ON_WRITE");
        properties.setProperty("nonPartition","false");
        properties.setProperty("multiPartitionKeys","false");
        properties.setProperty("sparkCustomCommand",sparkCustomCommand);

        properties.setProperty("getSourceTableSQL","false");
        properties.setProperty("getCloneTableSQL","false");
        properties.setProperty("createTableInClone","false");
        properties.setProperty("executeExportScript","false");
        properties.setProperty("executeLoadScript","false");
        properties.setProperty("csvFileName","");

        properties.setProperty("deleteCache","false");
        properties.setProperty("tempDirectory","./TCM-TempData/");
        properties.setProperty("delimiter","|");
        properties.setProperty("debug","true");

        return properties;
    }


    public static TableCloneManageConfig getDefTcmConfig(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        return TableCloneManageConfig.getInstance().loadLocalConfig(getProperties(sourceConfig,cloneConfig));
    }

    public static TableCloneManageContext getTCMC(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig){
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        return tcmcBuilder
                .setTcmConfig(getDefTcmConfig(sourceConfig,cloneConfig))
                .create();
    }

    public static TableCloneManage getTCM(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig) {
        return TableCloneManageFactory.createTableCloneManage(getTCMC(sourceConfig,cloneConfig));
    }
    public static TableCloneManage getTCM(TableCloneManageContext tcmc) {
        return TableCloneManageFactory.createTableCloneManage(tcmc);
    }

    public static DatabaseConfig getConfigMySQL() {
        return configMySQL;
    }

    public static DatabaseConfig getConfigPGSQL() {
        return configPGSQL;
    }

    public static DatabaseConfig getConfigHudi() {
        return configHudi;
    }

    public static List<Column> getColumns(DataSourceType type){
        List<Column> columns = new LinkedList<>();

        columns.add(new Column().setColumnName("col_boolean")
                .setDataType(TableCloneManageType.BOOLEAN.getOutDataType(type))
                .setOrdinalPosition(0)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.BOOLEAN));
        columns.add(new Column().setColumnName("col_float")
                .setDataType(TableCloneManageType.FLOAT32.getOutDataType(type))
                .setNumericPrecisionM(65)
                .setNumericPrecisionD(30)
                .setOrdinalPosition(1)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.FLOAT32));
        columns.add(new Column().setColumnName("col_double")
                .setDataType(TableCloneManageType.FLOAT64.getOutDataType(type))
                .setNumericPrecisionM(65)
                .setNumericPrecisionD(30)
                .setOrdinalPosition(2)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.FLOAT64));
        columns.add(new Column().setColumnName("col_bytes")
                .setDataType(TableCloneManageType.BYTES.getOutDataType(type))
                .setCharMaxLength(255L)
                .setOrdinalPosition(3)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.BYTES));
        columns.add(new Column().setColumnName("col_time")
                .setDataType(TableCloneManageType.TIME.getOutDataType(type))
                .setDatetimePrecision(6)
                .setOrdinalPosition(4)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.TIME));
        columns.add(new Column().setColumnName("col_time_stamp")
                .setDataType(TableCloneManageType.TIMESTAMP.getOutDataType(type))
                .setDatetimePrecision(6)
                .setOrdinalPosition(5)
                .setNullAble(Boolean.TRUE).setTableCloneManageType(TableCloneManageType.TIMESTAMP));

        return columns;
    }

    public static Table getTable(DataSourceType type,String tableName){
        Table table = new Table();
        table.setDataSourceType(type)
                .setTableName(tableName)
                .setCatalogName("test_Catalog")
                .setSchemaName("test_Schema")
                .setColumns(getColumns(type))
                .setSourceType(type);
        return table;
    }
}
