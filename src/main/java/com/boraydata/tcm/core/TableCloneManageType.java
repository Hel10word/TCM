package com.boraydata.tcm.core;

/**
 * According Sink to deal with the datatype mapping relationship
 *
 * https://docs.confluent.io/3.1.1/connect/connect-jdbc/docs/sink_connector.html#auto-creation-and-auto-evoluton
 *
 * @author bufan
 * @data 2021/8/30
 */
public enum TableCloneManageType {
//  tcm Type        Type name       default MySQL type              default PgSQL type
    INT8        ("INT8",        "TINYINT",          "SMALLINT",         "SMALLINT"),
    INT16       ("INT16",       "SMALLINT",         "SMALLINT",         "SMALLINT"),
    INT32       ("INT32",       "INT",              "INT",              "INT"),
    INT64       ("INT64",       "BIGINT",           "BIGINT",           "BIGINT"),
    FLOAT32     ("FLOAT32",     "FLOAT",            "REAL",             "FLOAT"),
    FLOAT64     ("FLOAT64",     "DOUBLE",           "DOUBLE PRECISION", "DOUBLE"),
    BOOLEAN     ("BOOLEAN",     "TINYINT(1)",          "BOOLEAN",          "BOOLEAN"),
    STRING      ("STRING",      "VARCHAR",          "TEXT",             "STRING"),
    BYTES       ("BYTES",       "VARBINARY",        "BYTEA",            "BINARY"),
    DECIMAL     ("'Decimal'",   "DECIMAL",          "DECIMAL",          "DECIMAL"),
    DATE        ("'Date'",      "DATE",             "DATE",             "DATE"),
    TIME        ("'Time'",      "TIME",             "INTERVAL",         "TIMESTAMP"),
    TIMESTAMP   ("'Timestamp'", "TIMESTAMP",        "TIMESTAMP",        "TIMESTAMP"),
    TEXT        ("TEXT",        "LONGTEXT",         "TEXT",             "STRING"),
    MONEY       ("MONEY",       "DECIMAL(65,2)",    "MONEY",            "DECIMAL")

    // the STRUCT usually used to represent {GEOMETRY, LINESTRING, POLYGON....}
    // This part is TCM self-defined, if the database does not support this datatype, please explain in the README.md
//    STRUCT      ("STRUCT",      "MULTIPOLYGON",     "POLYGON",          "STRUCT");
    ;

    String value;
    String mysql;
    String pgsql;
    String spark;
    TableCloneManageType(String value, String mysql, String pgsql, String spark){
        this.value = value;
        this.mysql = mysql;
        this.pgsql = pgsql;
        this.spark = spark;
    }

    public String getOutDataType(DataSourceType dst){
        if(DataSourceType.MYSQL.toString().equals(dst.toString()))
            return mysql;
        else if(DataSourceType.POSTGRES.toString().equals(dst.toString()))
            return pgsql;
//        else if(DataSourceType.SPARK.toString().equals(dst.toString()))
//            return spark;
        return null;
    }

}
