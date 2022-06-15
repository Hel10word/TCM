package com.boraydata.cdc.tcm;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.core.*;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;

import java.util.LinkedList;
import java.util.List;

/**
 * @author bufan
 * @date 2021/11/5
 */
public class TestDataProvider {
    public static final String CUSTOM_FILE_PATH = "./customTable.json";
    public static final String JSON_CONFIG_FILE_PATH = "./config.json";
    public static final String PROPERTIES_CONFIG_FILE_PATH = "./config.properties";
    public static final String SPARK_CUSTOM_COMMAND = "spark-shell \\\n" +
            "            --jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \\\n" +
            "            --driver-class-path $HADOOP_CONF_DIR \\\n" +
            "            --packages org.apache.spark:spark-avro_2.11:2.4.4 \\\n" +
            "            --master local[2] \\\n" +
            "            --deploy-mode client \\\n" +
            "            --driver-memory 2G \\\n" +
            "            --executor-memory 2G \\\n" +
            "            --num-executors 2";

    //========================== MySQL ===============================
    public static DatabaseConfig MySQLConfig = new DatabaseConfig()
            .setDatabaseName("test_db")
            .setDataSourceEnum(DataSourceEnum.MYSQL)
            .setHost("192.168.30.38")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root");

    //========================== PgSQL ===============================
    public static DatabaseConfig PostgreSQLConfig = new DatabaseConfig()
            .setDatabaseName("test_db")
            .setDataSourceEnum(DataSourceEnum.POSTGRESQL)
            .setHost("192.168.30.38")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("postgres");

    public static DatabaseConfig SQLServerConfig = new DatabaseConfig()
            .setDatabaseName("test_db")
            .setDataSourceEnum(DataSourceEnum.SQLSERVER)
            .setHost("192.168.120.237")
            .setPort("1433")
            .setUsername("sa")
            .setPassword("Rapids123*");

    //========================== Hudi ===============================
    public static DatabaseConfig HudiConfig = new DatabaseConfig()
            .setDatabaseName("test_cdc_hudi")
            .setDataSourceEnum(DataSourceEnum.HUDI)
            .setHost("192.168.120.67")
            .setPort("10000")
            .setUsername("rapids")
            .setPassword("rapids");




    public static TableCloneManagerConfig getTCMConfig(){
        return setHudiConfig(new TableCloneManagerConfig()
                .setSourceConfig(MySQLConfig)
                .setCloneConfig(PostgreSQLConfig)
                .setSourceTableName("mysql_table")
                .setCloneTableName("postgresql_table"),
                "id");
    }
    public static TableCloneManagerConfig setHudiConfig(TableCloneManagerConfig config, String primaryKey){
        return config
//                .setCustomSchemaFilePath(CUSTOM_FILE_PATH)
                .setHdfsSourceDataDir("hdfs:///cdc-init/")
                .setHdfsCloneDataPath("hdfs:///cdc-init/temple_table")
                .setPrimaryKey(primaryKey)
                .setSparkCustomCommand(SPARK_CUSTOM_COMMAND);
    }

    public static TableCloneManagerConfig getTCMConfig(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig){
        return getTCMConfig().setSourceConfig(sourceConfig).setCloneConfig(cloneConfig).checkConfig();
    }

    public static TableCloneManagerContext getTCMContext(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig){
        TableCloneManagerContext.Builder tcmcBuilder = new TableCloneManagerContext.Builder();
        return tcmcBuilder
                .setTcmConfig(getTCMConfig(sourceConfig,cloneConfig))
                .create();
    }

    public static TableCloneManager getTCM(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig) {
        return TableCloneManagerFactory.createTableCloneManage(getTCMContext(sourceConfig,cloneConfig));
    }

//    public static DatabaseConfig getMySQLConfig() {
//        return MySQLConfig;
//    }
//
//    public static DatabaseConfig getPostgreSQLConfig() {
//        return PostgreSQLConfig;
//    }
//
//    public static DatabaseConfig getHudiConfig() {
//        return HudiConfig;
//    }

    public static List<Column> getColumns(DataSourceEnum type){
        List<Column> columns = new LinkedList<>();
        columns.add(new Column().setColumnName("col_tinyint")
                .setDataType(TCMDataTypeEnum.INT8.getMappingDataType(type))
                .setNumericPrecision(10)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(1)
                .setTCMDataTypeEnum(TCMDataTypeEnum.INT8));
        columns.add(new Column().setColumnName("col_smallint")
                .setDataType(TCMDataTypeEnum.INT16.getMappingDataType(type))
                .setNumericPrecision(10)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(2)
                .setTCMDataTypeEnum(TCMDataTypeEnum.INT16));
        columns.add(new Column().setColumnName("col_integer")
                .setDataType(TCMDataTypeEnum.INT32.getMappingDataType(type))
                .setNumericPrecision(10)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(3)
                .setTCMDataTypeEnum(TCMDataTypeEnum.INT32));
        columns.add(new Column().setColumnName("col_bigint")
                .setDataType(TCMDataTypeEnum.INT64.getMappingDataType(type))
                .setNumericPrecision(10)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(4)
                .setTCMDataTypeEnum(TCMDataTypeEnum.INT64));
        columns.add(new Column().setColumnName("col_float")
                .setDataType(TCMDataTypeEnum.FLOAT32.getMappingDataType(type))
                .setNumericPrecision(30)
                .setNumericScale(12)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(5)
                .setTCMDataTypeEnum(TCMDataTypeEnum.FLOAT32));
        columns.add(new Column().setColumnName("col_double")
                .setDataType(TCMDataTypeEnum.FLOAT64.getMappingDataType(type))
                .setNumericPrecision(65)
                .setNumericScale(30)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(6)
                .setTCMDataTypeEnum(TCMDataTypeEnum.FLOAT64));
        columns.add(new Column().setColumnName("col_boolean")
                .setDataType(TCMDataTypeEnum.BOOLEAN.getMappingDataType(type))
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(7)
                .setTCMDataTypeEnum(TCMDataTypeEnum.BOOLEAN));
        columns.add(new Column().setColumnName("col_varchar")
                .setDataType(TCMDataTypeEnum.STRING.getMappingDataType(type))
                .setCharacterMaximumPosition(255L)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(8)
                .setTCMDataTypeEnum(TCMDataTypeEnum.STRING));
        columns.add(new Column().setColumnName("col_bytes")
                .setDataType(TCMDataTypeEnum.BYTES.getMappingDataType(type))
                .setCharacterMaximumPosition(255L)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(9)
                .setTCMDataTypeEnum(TCMDataTypeEnum.BYTES));
        columns.add(new Column().setColumnName("col_decimal")
                .setDataType(TCMDataTypeEnum.DECIMAL.getMappingDataType(type))
                .setNumericPrecision(65)
                .setNumericScale(30)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(10)
                .setTCMDataTypeEnum(TCMDataTypeEnum.DECIMAL));
        columns.add(new Column().setColumnName("col_date")
                .setDataType(TCMDataTypeEnum.DATE.getMappingDataType(type))
                .setDatetimePrecision(0)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(11)
                .setTCMDataTypeEnum(TCMDataTypeEnum.DATE));
        columns.add(new Column().setColumnName("col_time")
                .setDataType(TCMDataTypeEnum.TIME.getMappingDataType(type))
                .setDatetimePrecision(3)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(12)
                .setTCMDataTypeEnum(TCMDataTypeEnum.TIME));
        columns.add(new Column().setColumnName("col_time_stamp")
                .setDataType(TCMDataTypeEnum.TIMESTAMP.getMappingDataType(type))
                .setDatetimePrecision(6)
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(13)
                .setTCMDataTypeEnum(TCMDataTypeEnum.TIMESTAMP));
        columns.add(new Column().setColumnName("col_long_string")
                .setDataType(TCMDataTypeEnum.TEXT.getMappingDataType(type))
                .setNullable(Boolean.TRUE)
                .setOrdinalPosition(14)
                .setTCMDataTypeEnum(TCMDataTypeEnum.TEXT));
        return columns;
    }

    public static List<Column> getCustomColumns() {
        List<Column> columns = new LinkedList<>();
        columns.add(new Column().setColumnName("col_varchar")
                .setCharacterMaximumPosition(255L)
                .setNullable(Boolean.FALSE)
                .setTCMDataTypeEnum(TCMDataTypeEnum.STRING));
        columns.add(new Column().setColumnName("col_decimal")
                .setNumericPrecision(65)
                .setNumericScale(30)
                .setNullable(Boolean.TRUE)
                .setTCMDataTypeEnum(TCMDataTypeEnum.DECIMAL));
        columns.add(new Column().setColumnName("col_time_stamp")
                .setDatetimePrecision(6)
                .setNullable(Boolean.TRUE)
                .setTCMDataTypeEnum(TCMDataTypeEnum.TIMESTAMP));
        return columns;
    }

    public static Table getTable(DataSourceEnum type, String tableName){
        Table table = new Table();
        table.setDataSourceEnum(type)
                .setTableName(tableName)
                .setCatalogName("test_Catalog")
                .setSchemaName("test_Schema")
                .setColumns(getColumns(type))
                .setOriginalDataSourceEnum(type);
        return table;
    }

    public static Table getCustomTable(){
        return new Table().setTableName("customTable").setColumns(getCustomColumns());
    }
}
