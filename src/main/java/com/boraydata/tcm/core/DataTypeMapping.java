package com.boraydata.tcm.core;

/**
 * According Sink to deal with the datatype mapping relationship
 *
 * https://docs.confluent.io/3.1.1/connect/connect-jdbc/docs/sink_connector.html#auto-creation-and-auto-evoluton
 *
 * @author bufan
 * @data 2021/8/30
 */
public enum DataTypeMapping {
//  tcm Type        Type name       default MySQL type              default PgSQL type
    INT8        ("INT8",        "TINYINT",          "SMALLINT"),
    INT16       ("INT16",       "SMALLINT",         "SMALLINT"),
    INT32       ("INT32",       "INT",              "INT"),
    INT64       ("INT64",       "BIGINT",           "BIGINT"),
    FLOAT32     ("FLOAT32",     "FLOAT",            "REAL"),
    FLOAT64     ("FLOAT64",     "DOUBLE",           "DOUBLE PRECISION"),
    BOOLEAN     ("BOOLEAN",     "TINYINT",          "BOOLEAN"),
    STRING      ("STRING",      "VARCHAR(256)",     "TEXT"),
    BYTES       ("BYTES",       "VARBINARY(1024)",  "BYTEA"),
    DECIMAL     ("'Decimal'",   "DECIMAL(65,s)",    "DECIMAL"),
    DATE        ("'Date'",      "DATE",             "DATE"),
    TIME        ("'Time'",      "TIME(3)",          "TIME"),
    TIMESTAMP   ("'Timestamp'", "TIMESTAMP(3)",     "TIMESTAMP"),

    // the STRUCT usually used to represent {GEOMETRY, LINESTRING, POLYGON....}
    // This part is TCM self-defined, if the database does not support this datatype, please explain in the README.md
    STRUCT   ("STRUCT", "MULTIPOLYGON",     "POLYGON");

    String value;
    String mysql;
    String pgsql;
    DataTypeMapping( String value,String mysql,String pgsql){
        this.value = value;
        this.mysql = mysql;
        this.pgsql = pgsql;
    }

    public String getOutDataType(DataSourceType dst){
        if(DataSourceType.MYSQL.name().equals(dst.name()))
            return mysql;
        else if(DataSourceType.POSTGRES.name().equals(dst.name()))
            return pgsql;
        return null;
    }

}
