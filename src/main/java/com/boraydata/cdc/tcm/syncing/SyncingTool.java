package com.boraydata.cdc.tcm.syncing;

/**
 * define some interfaces for generate load and export command
 * @author bufan
 * @date 2021/9/24
 */

import com.boraydata.cdc.tcm.core.TableCloneManagerContext;

/**
 * e.g.
 * MySQL Export
 * mysqlsh -h192.168.30.244 -P3306 -uroot -proot --database test_db -e "util.exportTable('lineitem_mysql','/usr/local/test.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})"
 * MySQL Load
 * mysql -h 192.168.30.200 -P 3306 -uroot -proot --database test_db -e "load data local infile '/usr/local/lineitem_1_limit_10.csv' into table lineitem fields terminated by ',' lines terminated by '\n';"
 *
 * PostgreSQL Export
 * psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with csv;"
 * PostgreSQL Load
 * psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with csv;"
 *
 */
public interface SyncingTool{
    // generate export table data information
    String getExportInfo(TableCloneManagerContext tcmContext);
    // generate load table data information
    String getLoadInfo(TableCloneManagerContext tcmContext);

    // execute export data operate
    Boolean executeExport(TableCloneManagerContext tcmContext);
    // execute load data operate
    Boolean executeLoad(TableCloneManagerContext tcmContext);
}
