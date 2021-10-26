

-- 测试用表 lineitem

CREATE TABLE lineitem ( l_orderkey    integer not null,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);

-- mysql 导入数据
mysql -uroot -proot --database=test_db -e "LOAD DATA INFILE '/usr/local/download/sf1/lineitem.tbl' INTO TABLE lineitem_sf1_mysql FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';"
mysql -uroot -proot --database=test_db -e "LOAD DATA INFILE '/usr/local/download/sf10/lineitem.tbl' INTO TABLE lineitem_sf10_mysql FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';"

-- PgSQL 导入数据