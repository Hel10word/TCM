package com.boraydata.tcm;

/**
 * @author bufan
 * @data 2021/9/2
 */
public enum CreateTableSQL {
    Pgsql(
            "CREATE TABLE robin_types_full(\n" +
                    "                          col_bigint bigint,\n" +
                    "                          col_bigserial bigserial,\n" +
                    "                          col_bit bit,\n" +
                    "                          col_bitvarying bit varying,\n" +
                    "                          col_boolean boolean,\n" +
                    "                          col_box box,\n" +
                    "                          col_bytea bytea,\n" +
                    "                          col_character character(10),\n" +
                    "                          col_charactervarying character varying(100),\n" +
                    "                          col_cidr cidr,\n" +
                    "                          col_circle circle,\n" +
                    "                          col_date date,\n" +
                    "                          col_doubleprecision double precision,\n" +
                    "                          col_inet inet,\n" +
                    "                          col_integer integer,\n" +
                    "                          col_interval interval,\n" +
                    "                          col_json json,\n" +
                    "                          col_jsonb jsonb,\n" +
                    "                          col_line line,\n" +
                    "                          col_lseg lseg,\n" +
                    "                          col_macaddr macaddr,\n" +
                    "                          col_macaddr8 macaddr8,\n" +
                    "                          col_money money,\n" +
                    "                          col_numeric numeric,\n" +
                    "                          col_path path,\n" +
                    "                          col_pg_lsn pg_lsn,\n" +
                    "                          col_point point,\n" +
                    "                          col_polygon polygon,\n" +
                    "                          col_real real,\n" +
                    "                          col_smallint smallint,\n" +
                    "                          col_smallserial smallserial,\n" +
                    "                          col_serial serial,\n" +
                    "                          col_text text,\n" +
                    "                          col_time time,\n" +
                    "                          col_timez time with time zone,\n" +
                    "                          col_timestamp timestamp,\n" +
                    "                          col_timestampz timestamp with time zone,\n" +
                    "                          col_tsquery tsquery,\n" +
                    "                          col_tsvector tsvector,\n" +
                    "                          col_txid_snapshot txid_snapshot,\n" +
                    "                          col_uuid uuid,\n" +
                    "                          col_xml xml\n" +
                    ");"
    ),
    Mysql(
            "Create Table If Not Exists `colume_type_1`(\n" +
                    "int_test INT\n" +
                    ",tinyint_test SMALLINT\n" +
                    ",smallint_test SMALLINT\n" +
                    ",float_test DOUBLE\n" +
                    ",double_test DOUBLE\n" +
                    ",decimal_test DOUBLE\n" +
                    ",char_test VARCHAR(256)\n" +
                    ",varchar_test VARCHAR(256)\n" +
                    ",text_test VARCHAR(256)\n" +
                    ",mediumtext_test VARCHAR(256)\n" +
                    ",date_test INT\n" +
                    ",time_test BIGINT\n" +
                    ",datetime_test BIGINT\n" +
                    ",linestring_test MULTIPOLYGON\n" +
                    ")Engine InnoDB;"
    );

    String CreateTableSQL;

    CreateTableSQL(String sql){
        this.CreateTableSQL = sql;
    }

    public String getCreateTableSQL() {
        return CreateTableSQL;
    }

    public CreateTableSQL setCreateTableSQL(String createTableSQL) {
        CreateTableSQL = createTableSQL;
        return this;
    }
}
