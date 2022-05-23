package com.boraydata.cdc.tcm.common.enums;

/**
 * data type mapping relationship
 * refer to:
 * @see <a href="https://docs.confluent.io/3.1.1/connect/connect-jdbc/docs/sink_connector.html#auto-creation-and-auto-evoluton"></a>
 *
 * @author bufan
 * @data 2021/8/30
 */

import com.boraydata.cdc.tcm.syncing.util.ScalaScriptGenerateUtil;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TCMDataTypeEnum {
//  tcm Type        Type name       default MySQL type              default PgSQL type
    INT8        ("INT8",        "TINYINT",          "SMALLINT",         "SMALLINT",      "IntegerType"),
    INT16       ("INT16",       "SMALLINT",         "SMALLINT",         "SMALLINT",      "IntegerType"),
    INT32       ("INT32",       "INT",              "INT",              "INT",           "IntegerType"),
    INT64       ("INT64",       "BIGINT",           "BIGINT",           "BIGINT",        "LongType"),
    FLOAT32     ("FLOAT32",     "FLOAT",            "REAL",             "FLOAT",         "FloatType"),
    FLOAT64     ("FLOAT64",     "DOUBLE",           "DOUBLE PRECISION", "DOUBLE",        "DoubleType"),
    BOOLEAN     ("BOOLEAN",     "TINYINT(1)",       "BOOLEAN",          "BOOLEAN",       "BooleanType"),
    STRING      ("STRING",      "VARCHAR",          "VARCHAR",          "STRING",        "StringType"),
    /**
     * For Hudi, BinaryType was used at first, but later tests found that Spark's Scala don‘t support this type.
     * @see <a href="https://stackoverflow.com/questions/45909622/binarytype-support-in-spark-with-scala">refer to</a>
     * I tried to use ByteType. Since the binary data exported by mysql is formatted as hexadecimal, result spark.read.schema(schema) reads csv data is null in all row
     * so use StringType.
     */
    BYTES       ("BYTES",       "VARBINARY",        "BYTEA",            "BINARY",        "StringType"),
    DECIMAL     ("DECIMAL",     "DECIMAL",          "DECIMAL",          "DECIMAL",       "StringType"),
    DATE        ("DATE",        "DATE",             "DATE",             "DATE",          "DateType"),
    TIME        ("TIME",        "TIME",             "INTERVAL",         "TIMESTAMP",     "TimestampType"),
    TIMESTAMP   ("TIMESTAMP",   "TIMESTAMP",        "TIMESTAMP",        "TIMESTAMP",     "TimestampType"),
    TEXT        ("TEXT",        "LONGTEXT",         "TEXT",             "STRING",        "StringType"),

    ;

    /**
     * Clone table data written to Hudi mainly use scala scripts to call the spark engine,
     * spark engine use some like 'hudi-spark-bundle_2.11-0.9.0.jar' JAR to perform related operations,
     * the data types corresponding to Spark are written here.
     * {@link ScalaScriptGenerateUtil}
     * @see <a href="https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/sql/types/"></a>
     *
     * @author: bufan
     * @date: 2022/5/16
     */
    @JsonValue
    String value;
    String mysql;
    String postgresql;
    String hive;
    String hudi;
    TCMDataTypeEnum(String value, String mysql, String postgresql, String hive, String hudi){
        this.value = value;
        this.mysql = mysql;
        this.postgresql = postgresql;
        this.hive = hive;
        this.hudi = hudi;
    }

    public String getMappingDataType(DataSourceEnum dst){
        if(DataSourceEnum.MYSQL.toString().equals(dst.toString()))
            return mysql;
        else if(DataSourceEnum.POSTGRESQL.toString().equals(dst.toString()))
            return postgresql;
//        else if(DataSourceEnum.HIVE.toString().equals(dst.toString()))
//            return hive;
        else if (DataSourceEnum.HUDI.toString().equals(dst.toString()))
            return hudi;
        return null;
    }

}
