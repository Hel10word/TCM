package com.boraydata.tcm.command;

/** define some interfaces for generate load and export command
 * @author bufan
 * @data 2021/9/24
 */

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.entity.Table;

/**
 * MySQL Export
 * mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -sN -e "select * from lineitem_1 limit 10"  | sed 's/"/""/g;s/\t/,/g;s/\n//g' > /usr/local/lineitem_1_limit_10.csv
 * MySQL Load
 * mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
 *
 * PgSQL Export
 * psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with csv;"
 * PgSQL Load
 * psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with csv;"
 *
 */
public interface CommandGenerate {
    // need： DatabaseConfig、File Path、Table Name、SQL statements。
    String exportCommand(DatabaseConfig config, String filePath,Table table,String delimiter);
    String exportCommand(DatabaseConfig config,String filePath,Table table,String delimiter,String limit);
    String loadCommand(DatabaseConfig config,String filePath,String tableName,String delimiter);
}
