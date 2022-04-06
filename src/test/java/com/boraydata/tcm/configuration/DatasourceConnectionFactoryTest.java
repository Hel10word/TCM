package com.boraydata.tcm.configuration;

import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.configuration.DatabaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test the database query by JDBC Connect
 * @author bufan
 * @data 2021/8/25
 */
class DatasourceConnectionFactoryTest {

    DatabaseConfig.Builder builderMySQL = new DatabaseConfig.Builder();
    DatabaseConfig configMySQL = builderMySQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.MYSQL)
            .setHost("192.168.30.38")
            .setPort("3306")
            .setUsername("root")
            .setPassword("root")
            .create();


    DatabaseConfig.Builder builderPgSQL = new DatabaseConfig.Builder();
    DatabaseConfig configPgSQL = builderPgSQL
            .setDatabasename("test_db")
            .setDataSourceType(DataSourceType.POSTGRES)
            .setHost("192.168.30.31")
            .setPort("5432")
            .setUsername("root")
            .setPassword("root")
            .create();

    DatabaseConfig.Builder builderHive = new DatabaseConfig.Builder();
    DatabaseConfig configHive = builderHive
            .setDatabasename("test_cdc_hudi")
            .setDataSourceType(DataSourceType.HIVE)
            .setHost("192.168.120.67")
            .setPort("10000")
            .setUsername("")
            .setPassword("")
            .create();
    @Test
    public void getJDBCUrlTest(){
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configMySQL));
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configPgSQL));
        System.out.println(DatasourceConnectionFactory.getJDBCUrl(configHive));

    }

    @Test
    public void executeQuerySQLTest(){
        // http://www.postgres.cn/docs/12/infoschema-columns.html
//        String sql = "select * from information_schema.COLUMNS where table_name in ('lineitem_sf1')";
//        String sql = "show databases";
        String sql = "show tables";
//        String sql = "drop table if exists customer_ro";
//        String sql = "drop table if exists customer_ro;drop table if exists customer_rt;";
//        String sql = configMySQL.getDataSourceType().SQL_AllTableInfo;
//        String sql = configMySQL.getDataSourceType().SQL_TableInfoByTableName.replace("?","'boolean_mysql'");
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
//        List list = DatasourceConnectionFactory.executeQuerySQL(configPgSQL, sql);
//        List list = DatasourceConnectionFactory.executeQuerySQL(configHive, sql);
//        list.forEach(System.out::println);


//        DatasourceConnectionFactory.showQueryBySQL(configMySQL, sql);
//        DatasourceConnectionFactory.showQueryBySQL(configPgSQL, sql);
        DatasourceConnectionFactory.showQueryBySQL(configHive, sql);

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
//        Connection con = DatasourceConnectionFactory.createDataSourceConnection(configPgSQL);
        Connection con = DatasourceConnectionFactory.createDataSourceConnection(configHive);
        con.close();
    }


}