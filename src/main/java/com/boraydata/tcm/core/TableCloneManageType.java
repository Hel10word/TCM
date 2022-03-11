package com.boraydata.tcm.core;

/**
 * According Sink to deal with the datatype mapping relationship
 *
 * https://docs.confluent.io/3.1.1/connect/connect-jdbc/docs/sink_connector.html#auto-creation-and-auto-evoluton
 *
 * @author bufan
 * @data 2021/8/30
 */
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
 *
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
 *
 * TimestampType 类型的数据会被转为 Bigint 类型来存储
 * https://stackoverflow.com/questions/57945174/how-to-convert-timestamp-to-bigint-in-a-pyspark-dataframe
 *
 */
public enum TableCloneManageType {
//  tcm Type        Type name       default MySQL type              default PgSQL type
    INT8        ("INT8",        "TINYINT",          "SMALLINT",         "SMALLINT",      "IntegerType"),
    INT16       ("INT16",       "SMALLINT",         "SMALLINT",         "SMALLINT",      "IntegerType"),
    INT32       ("INT32",       "INT",              "INT",              "INT",           "IntegerType"),
    INT64       ("INT64",       "BIGINT",           "BIGINT",           "BIGINT",        "LongType"),
    FLOAT32     ("FLOAT32",     "FLOAT",            "REAL",             "FLOAT",         "FloatType"),
    FLOAT64     ("FLOAT64",     "DOUBLE",           "DOUBLE PRECISION", "DOUBLE",        "DoubleType"),
    BOOLEAN     ("BOOLEAN",     "TINYINT(1)",       "BOOLEAN",          "BOOLEAN",       "BooleanType"),
    STRING      ("STRING",      "VARCHAR",          "VARCHAR",             "STRING",        "StringType"),
    BYTES       ("BYTES",       "VARBINARY",        "BYTEA",            "BINARY",        "BinaryType"),
    DECIMAL     ("'Decimal'",   "DECIMAL",          "DECIMAL",          "DECIMAL",       "StringType"),
    DATE        ("'Date'",      "DATE",             "DATE",             "DATE",          "DateType"),
    TIME        ("'Time'",      "TIME",             "INTERVAL",         "TIMESTAMP",     "TimestampType"),
    TIMESTAMP   ("'Timestamp'", "TIMESTAMP",        "TIMESTAMP",        "TIMESTAMP",     "TimestampType"),
    TEXT        ("TEXT",        "LONGTEXT",         "TEXT",             "STRING",        "StringType"),
    MONEY       ("MONEY",       "DECIMAL(65,2)",    "MONEY",            "DECIMAL",       "StringType")

    // the STRUCT usually used to represent {GEOMETRY, LINESTRING, POLYGON....}
    // This part is TCM self-defined, if the database does not support this datatype, please explain in the README.md
//    STRUCT      ("STRUCT",      "MULTIPOLYGON",     "POLYGON",          "STRUCT");
    ;

    String value;
    String mysql;
    String pgsql;
    String spark;
    String hudi;
    TableCloneManageType(String value, String mysql, String pgsql, String spark,String hudi){
        this.value = value;
        this.mysql = mysql;
        this.pgsql = pgsql;
        this.spark = spark;
        this.hudi = hudi;
    }

    public String getOutDataType(DataSourceType dst){
        if(DataSourceType.MYSQL.toString().equals(dst.toString()))
            return mysql;
        else if(DataSourceType.POSTGRES.toString().equals(dst.toString()))
            return pgsql;
//        else if(DataSourceType.SPARK.toString().equals(dst.toString()))
//            return spark;
        else if (DataSourceType.HUDI.toString().equals(dst.toString()))
            return hudi;
        return null;
    }

}
