package com.boraydata.tcm.utils;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;

import java.sql.*;

/** Test to Connect Spark
 * @author bufan
 * @data 2021/10/11
 *
 * refer to
 * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
 * https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC
 */
public class SparkConnectTest {

    DatabaseConfig.Builder sparkBuilder = new DatabaseConfig.Builder();
    DatabaseConfig sparkConfig = sparkBuilder
            .setDataSourceType(DataSourceType.SPARK)
            .setDatabasename("test_db")
            .setHost("192.168.30.221")
            .setPort("10000")
            .setUsername("")
            .setPassword("")
            .create();

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    @Test
    public void sparkConnector() throws SQLException {

//        try {
//            Class.forName(driverName);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.30.221:10000", "", "");

        Connection con = DatasourceConnectionFactory.createDataSourceConnection(sparkConfig);

        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery("show tables;");
        while (res.next())
            System.out.println(res.getString(1));
    }

}
