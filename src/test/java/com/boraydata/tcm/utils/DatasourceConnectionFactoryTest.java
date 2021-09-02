package com.boraydata.tcm.utils;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.configuration.DatabaseConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/** Test the database query by JDBC Connect
 * @author bufan
 * @data 2021/8/25
 */
class DatasourceConnectionFactoryTest {

    //======================== MYSQL =====================================
    @Test
    public void mysqlConnTest() {
        DatabaseConfig.Builder builder = new DatabaseConfig.Builder();
        DatabaseConfig config = builder
                .setDatabasename("test_db")
                .setDataSourceType(DataSourceType.MYSQL)
                .setHost("192.168.30.192")
                .setPort("3306")
                .setUsername("root")
                .setPassword("root")
                .create();
        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(config);
                PreparedStatement ps = conn.prepareStatement(config.getDataSourceType().SQL_TableInfoByTableName)
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
        DatabaseConfig.Builder builder = new DatabaseConfig.Builder();
        DatabaseConfig config = builder
                .setDatabasename("test_db")
                .setDataSourceType(DataSourceType.POSTGRES)
                .setHost("192.168.30.192")
                .setPort("5432")
                .setUsername("postgres")
                .setPassword("")
                .create();
        try (
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(config);
                PreparedStatement ps = conn.prepareStatement(config.getDataSourceType().SQL_TableInfoByTableName);
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