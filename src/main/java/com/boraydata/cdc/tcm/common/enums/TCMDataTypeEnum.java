package com.boraydata.cdc.tcm.common.enums;

/**
 *
 * @author bufan
 * @date 2021/8/30
 * !!!!!!!!!!!!!!!!!!!!!!!!!! Note !!!!!!!!!!!!!!!!!!!!!
 * For Hudi, BinaryType was used at first, but later tests found that Spark's Scala don‘t support this type.
 * @see <a href="https://stackoverflow.com/questions/45909622/binarytype-support-in-spark-with-scala">refer to</a>
 * I tried to use ByteType. Since the binary data exported by mysql is formatted as hexadecimal, result spark.read.schema(schema) reads csv data is null in all row
 * so use StringType.
 */

import com.boraydata.cdc.tcm.syncing.util.ScalaScriptGenerateUtil;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TCMDataTypeEnum {
//  tcm Type        Type name       default MySQL type              default PostgreSQL type          default SQL Server type     default HIVE type        default Scala DataFrame type
    INT8        ("INT8",        "TINYINT",          "SMALLINT",         "TINYINT",          "SMALLINT",      "IntegerType"),
    INT16       ("INT16",       "SMALLINT",         "SMALLINT",         "SMALLINT",         "SMALLINT",      "IntegerType"),
    INT32       ("INT32",       "INTEGER",          "INT",              "INT",              "INT",           "IntegerType"),
    INT64       ("INT64",       "BIGINT",           "BIGINT",           "BIGINT",           "BIGINT",        "LongType"),
    FLOAT32     ("FLOAT32",     "FLOAT",            "REAL",             "REAL",             "FLOAT",         "FloatType"),
    FLOAT64     ("FLOAT64",     "DOUBLE",           "DOUBLE PRECISION", "FLOAT",            "DOUBLE",        "DoubleType"),
    BOOLEAN     ("BOOLEAN",     "TINYINT(1)",       "BOOLEAN",          "BIT",              "BOOLEAN",       "BooleanType"),
    STRING      ("STRING",      "VARCHAR",          "VARCHAR",          "NVARCHAR",         "STRING",        "StringType"),
    BYTES       ("BYTES",       "VARBINARY",        "BYTEA",            "VARBINARY",        "BINARY",        "StringType"),
    DECIMAL     ("DECIMAL",     "DECIMAL",          "DECIMAL",          "DECIMAL",          "DECIMAL",       "StringType"),
    DATE        ("DATE",        "DATE",             "DATE",             "DATE",             "DATE",          "DateType"),
    TIME        ("TIME",        "TIME",             "TIME",             "TIME",             "TIMESTAMP",     "TimestampType"),
    TIMESTAMP   ("TIMESTAMP",   "TIMESTAMP",        "TIMESTAMP",        "DATETIMEOFFSET",   "TIMESTAMP",     "TimestampType"),
    TEXT        ("TEXT",        "LONGTEXT",         "TEXT",             "NTEXT",            "STRING",        "StringType"),
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
    String sqlserver;
    String hive;
    String hudi;
    TCMDataTypeEnum(String value, String mysql, String postgresql,String sqlserver, String hive, String hudi){
        this.value = value;
        this.mysql = mysql;
        this.postgresql = postgresql;
        this.sqlserver = sqlserver;
        this.hive = hive;
        this.hudi = hudi;
    }

    public String getMappingDataType(DataSourceEnum dst){
        if(dst == null)
            return null;
        switch (dst){
            case MYSQL:
            case RPDSQL:
                return mysql;
            case POSTGRESQL:
                return postgresql;
            case SQLSERVER:
                return sqlserver;
            case HUDI:
                return hudi;
            default:
                return null;
        }
    }

}
