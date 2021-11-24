

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


-- mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e ""

truncate table lineitem_sf1_mysql_clone;

insert into lineitem_sf1_mysql_clone select l_orderkey,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,CASE L_LINESTATUS WHEN 'O' THEN '1' WHEN 'F' THEN '0' ELSE '0' END as L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT from lineitem_sf1_mysql limit 1;


create table lineitem_sf1_mysql_clone as select l_orderkey,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,CASE L_LINESTATUS WHEN 'O' THEN '1' WHEN 'F' THEN '0' ELSE '0' END as L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT from lineitem_sf1_mysql limit 1;


-- PgSQL 导入数据