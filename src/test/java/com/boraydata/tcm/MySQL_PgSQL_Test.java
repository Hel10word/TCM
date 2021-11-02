package com.boraydata.tcm;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.exception.TCMException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/** 测试 MySQL 与 PgSQL 间的表同步
 * @author bufan
 * @data 2021/9/22
 */
public class MySQL_PgSQL_Test {
    DatabaseConfig.Builder mysql = new DatabaseConfig.Builder();
    DatabaseConfig mysqlConfig = mysql
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.148")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    DatabaseConfig.Builder pgsql = new DatabaseConfig.Builder();
    DatabaseConfig pgsqlConfig = pgsql
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("postgres")
            .create();

    long start,end = 0;

    @Test
    public void test() {
        System.out.println("Clone table mysql_pg_table in PgSQL from MySQL.lineitem");
        start = System.currentTimeMillis();
        foo(mysqlConfig,pgsqlConfig,"string_types_mysql");
        end = System.currentTimeMillis();
        System.out.println("Total time spent:" + (end - start)+"\n\n");

//        System.out.println("Clone table pg_mysql_table in MySQL from PgSQL.lineitem");
//        start = System.currentTimeMillis();
//        foo(pgsqlConfig,mysqlConfig,"pg_mysql_table");
//        end = System.currentTimeMillis();
//        System.out.println("Total time spent:" + (end - start)+"\n\n");
//
//        System.out.println("Clone table mysql_mysql_table in MySQL from MySQL.lineitem");
//        start = System.currentTimeMillis();
//        foo(mysqlConfig,mysqlConfig,"mysql_mysql_table");
//        end = System.currentTimeMillis();
//        System.out.println("Total time spent:" + (end - start)+"\n\n");
//
//        System.out.println("Clone table pg_pg_table in PgSQL from PgSQL.lineitem");
//        start = System.currentTimeMillis();
//        foo(pgsqlConfig,pgsqlConfig,"pg_pg_table");
//        end = System.currentTimeMillis();
//        System.out.println("Total time spent:" + (end - start)+"\n\n");
    }





    public void foo(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,String tableName){
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmContext = tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .create();
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);

        Table sourceTable = tcm.getSourceTable(tableName);

        Table cloneTable = tcm.mappingCloneTable(sourceTable);

        cloneTable.setTablename(tableName);

        boolean flag = tcm.createTableInCloneDatasource(cloneTable);
        if(flag)
            System.out.println("create "+tableName+" Success");
        else
            System.out.println("Create "+tableName+" Failure");
    }

@Test
    public void testDB(){
        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(pgsqlConfig);
                PreparedStatement ps = conn.prepareStatement("/copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with csv;");
        ){
            ResultSet myResultSet = ps.executeQuery();
            while (myResultSet.next())
                System.out.println(
                        myResultSet.getString(1)+"   |    "+
                                myResultSet.getString(2)+"   |    "+
                                myResultSet.getString(3));
            myResultSet.close();
        }catch (Exception e) {
//            throw new TCMException("Failed to create PostgreSQL connection");
        }
    }
}
