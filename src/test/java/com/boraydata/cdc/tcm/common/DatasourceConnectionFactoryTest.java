package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.exception.TCMException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test the database query by JDBC Connect
 * @author bufan
 * @date 2021/8/25
 */
class DatasourceConnectionFactoryTest {

    DatabaseConfig configMySQL = TestDataProvider.MySQLConfig;
    DatabaseConfig configPostgreSQL = TestDataProvider.PostgreSQLConfig;
    DatabaseConfig configSqlServer = TestDataProvider.SQLServerConfig.setSchema("dbo");

    DatabaseConfig configHudi = TestDataProvider.HudiConfig.setDatabaseName("test_cdc_hudi");
    @Test
    public void getJDBCUrlTest(){
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configMySQL));
//        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configPostgreSQL));
//        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configHudi));
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configSqlServer));

    }

    @Test
    public void executeQuerySQLTest(){
        // http://www.postgres.cn/docs/12/infoschema-columns.html
//        String sql = "select * from information_schema.COLUMNS where table_name in ('lineitem_sf1')";
//        String sql = "show databases";
//        String sql = "select * from information_schema.schemata";
//        String sql = "show tables";
        String sql = "select * from test;";
//        String sql = "drop table if exists temple_table_rt";
//        String sql = "drop table if exists customer_ro;drop table if exists customer_rt;";
//        String sql = configMySQL.getDataSourceEnum().SQL_AllTableInfo;
//        String sql = configMySQL.getDataSourceEnum().SQL_TableInfoByTableName.replace("?","'boolean_mysql'");
//        String sql = "select * from lineitem_mysql";
//        lineitem_mysql
//        String sql = "select *\n" +
//                "from lineitem_mysql\n" +
//                "into outfile './test1.csv'\n" +
//                "FIELDS\n" +
//                "-- ENCLOSED BY '-'\n" +
//                "TERMINATED BY ','\n" +
//                "ESCAPED BY '\\\\'\n" +
//                "LINES TERMINATED BY '\\n';";
//        System.out.println(sql);
//        List list = DatasourceConnectionFactory.executeQuerySQL(configMySQL, sql);
//        List list = DatasourceConnectionFactory.executeQuerySQL(configPostgreSQL, sql);
//        List list = DatasourceConnectionFactory.executeQuerySQL(configHive, sql);
//        list.forEach(System.out::println);

//        String sql = "create table test(id int)";
        DatasourceConnectionFactory.showQueryBySQL(configMySQL, sql);
//        DatasourceConnectionFactory.showQueryBySQL(configPostgreSQL, sql);
//        DatasourceConnectionFactory.showQueryBySQL(configSqlServer, sql);
//        DatasourceConnectionFactory.showQueryBySQL(configHudi, sql);

//        System.out.println(list.get(0).toString());
    }

    //=========================== MYSQL =====================================
    @Test
    public void mysqlConnTest() {
        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(configMySQL);
                PreparedStatement ps = conn.prepareStatement(configMySQL.getDataSourceEnum().SQL_TableInfoByTableName)
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

    //=========================== PostgreSQL =====================================
    @Test
    public void postgresqlConnTest() {

        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(configPostgreSQL);
                PreparedStatement ps = conn.prepareStatement(configPostgreSQL.getDataSourceEnum().SQL_TableInfoByTableName);
        ){
//            ResultSet myResultSet = statement.executeQuery("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_name = 'test';");
            ps.setString(1,"lineitem_sf1");
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


    @Test
    public void connectionTest() throws SQLException {
//        Connection con = DatasourceConnectionFactory.createDataSourceConnection(configMySQL);
//        Connection con = DatasourceConnectionFactory.createDataSourceConnection(configPostgreSQL);
        Connection con = DatasourceConnectionFactory.createDataSourceConnection(configHudi);
        con.close();
    }


}