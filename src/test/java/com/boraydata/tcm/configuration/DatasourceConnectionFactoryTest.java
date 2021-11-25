package com.boraydata.tcm.configuration;

import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.configuration.DatabaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/** Test the database query by JDBC Connect
 * @author bufan
 * @data 2021/8/25
 */
class DatasourceConnectionFactoryTest {

    DatabaseConfig.Builder builderMySQL = new DatabaseConfig.Builder();
    DatabaseConfig configMySQL = builderMySQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.148")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();

    DatabaseConfig.Builder builderPgSQL = new DatabaseConfig.Builder();
    DatabaseConfig configPgSQL = builderPgSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.155")
            .setPort("5432")
            .setUsername("postgres")
            .setPassword("")
            .create();

    @Test
    public void getJDBCUrlTest(){
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configMySQL));
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configPgSQL));
    }

    @Test
    public void executeQuerySQLTest(){
        // http://www.postgres.cn/docs/12/infoschema-columns.html
        String sql = "select * from information_schema.COLUMNS where table_name in ('object_types_pgsql')";
//        String sql = configMySQL.getDataSourceType().SQL_AllTableInfo;
//        String sql = configMySQL.getDataSourceType().SQL_TableInfoByTableName.replace("?","'boolean_mysql'");
//        String sql = "select * from lineitem_mysql";
        System.out.println(sql);
        List list = DatasourceConnectionFactory.executeQuerySQL(configMySQL, sql);
//        List list = DatasourceConnectionFactory.executeQuerySQL(configPgSQL, sql);
        list.forEach(System.out::println);
//        System.out.println(list.get(0).toString());
    }

    //=========================== MYSQL =====================================
    @Test
    public void mysqlConnTest() {
        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(configMySQL);
                PreparedStatement ps = conn.prepareStatement(configMySQL.getDataSourceType().SQL_TableInfoByTableName)
                ){
            ps.setString(1,"colume_type");
            ResultSet myResultSet = ps.executeQuery();
            while (myResultSet.next())
                System.out.println(myResultSet.getString(1));
            myResultSet.close();
        }catch (Exception e) {
            throw new TCMException("Failed to create MySQL connection");
        }
    }

    //=========================== PGSQL =====================================
    @Test
    public void postgresqlConnTest() {

        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(configPgSQL);
                PreparedStatement ps = conn.prepareStatement(configPgSQL.getDataSourceType().SQL_TableInfoByTableName);
        ){
//            ResultSet myResultSet = statement.executeQuery("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_name = 'test';");
            ps.setString(1,"test");
            ResultSet myResultSet = ps.executeQuery();
            while (myResultSet.next())
                System.out.println(
                        myResultSet.getString(1)+"   |    "+
                        myResultSet.getString(2)+"   |    "+
                        myResultSet.getString(3));
            myResultSet.close();
        }catch (Exception e) {
            throw new TCMException("Failed to create PostgreSQL connection");
        }
    }




}