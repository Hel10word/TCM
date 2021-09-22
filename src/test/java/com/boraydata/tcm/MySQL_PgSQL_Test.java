package com.boraydata.tcm;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import org.junit.jupiter.api.Test;

/** 测试 MySQL 与 PgSQL 间的表同步
 * @author bufan
 * @data 2021/9/22
 */
public class MySQL_PgSQL_Test {
    DatabaseConfig.Builder mysql = new DatabaseConfig.Builder();
    DatabaseConfig mysqlConfig = mysql
            .setDatabasename("test")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.222")
            .setPort("3306")
            .setUsername("root")
            .setPassword("bigdata")
            .create();

    DatabaseConfig.Builder pgsql = new DatabaseConfig.Builder();
    DatabaseConfig pgsqlConfig = pgsql
            .setDatabasename("test")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.202")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("postgres")
            .create();

    long start,end = 0;

    @Test
    public void test() {
        System.out.println("Clone table mysql_pg_table in PgSQL from MySQL.lineitem");
        start = System.currentTimeMillis();
        foo(mysqlConfig,pgsqlConfig,"mysql_pg_table");
        end = System.currentTimeMillis();
        System.out.println("Total time spent:" + (end - start)+"\n\n");

        System.out.println("Clone table pg_mysql_table in MySQL from PgSQL.lineitem");
        start = System.currentTimeMillis();
        foo(pgsqlConfig,mysqlConfig,"pg_mysql_table");
        end = System.currentTimeMillis();
        System.out.println("Total time spent:" + (end - start)+"\n\n");

        System.out.println("Clone table mysql_mysql_table in MySQL from MySQL.lineitem");
        start = System.currentTimeMillis();
        foo(mysqlConfig,mysqlConfig,"mysql_mysql_table");
        end = System.currentTimeMillis();
        System.out.println("Total time spent:" + (end - start)+"\n\n");

        System.out.println("Clone table pg_pg_table in PgSQL from PgSQL.lineitem");
        start = System.currentTimeMillis();
        foo(pgsqlConfig,pgsqlConfig,"pg_pg_table");
        end = System.currentTimeMillis();
        System.out.println("Total time spent:" + (end - start)+"\n\n");
    }





    public void foo(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,String tableName){
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmContext = tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .create();
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);

        Table sourceTable = tcm.getSourceTable("lineitem");

        Table cloneTable = tcm.mappingCloneTable(sourceTable);

        cloneTable.setTablename(tableName);

        boolean flag = tcm.createTableInCloneDatasource(cloneTable);
        if(flag)
            System.out.println("create "+tableName+" Success");
        else
            System.out.println("Create "+tableName+" Failure");
    }
}
