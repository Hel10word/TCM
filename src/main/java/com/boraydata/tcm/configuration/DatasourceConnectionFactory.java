package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;

import java.sql.*;
import java.util.*;

/** Used to create some database connection objects
 * @author bufan
 * @data 2021/8/25
 */
public class DatasourceConnectionFactory {
    static Properties dscPropers;
    // define create datasource info
    static {
        dscPropers = new Properties();
        dscPropers.setProperty("ORACLE", "jdbc:oracle:thin:@");
        dscPropers.setProperty("ORACLE_Driver", "oracle.jdbc.driver.OracleDriver");
        dscPropers.setProperty("ORACLE_TAIL", ":");
        dscPropers.setProperty("DB2", "jdbc:db2://");
        dscPropers.setProperty("DB2_Driver", "com.ibm.db2.jcc.DB2Driver");
        dscPropers.setProperty("DB2_TAIL", ":");
        dscPropers.setProperty("MYSQL", "jdbc:mysql://");
        dscPropers.setProperty("MYSQL_Driver", "com.mysql.jdbc.Driver");
        dscPropers.setProperty("MYSQL_TAIL", "/");
        dscPropers.setProperty("POSTGRES", "jdbc:postgresql://");
        dscPropers.setProperty("POSTGRES_Driver", "org.postgresql.Driver");
        dscPropers.setProperty("POSTGRES_TAIL", "/");
        dscPropers.setProperty("VOLTDB", "jdbc:voltdb://");
        dscPropers.setProperty("VOLTDB_Driver", "org.voltdb.jdbc.Driver");
        dscPropers.setProperty("VOLTDB_TAIL", "/");
        dscPropers.setProperty("GREENPLUM", "jdbc:pivotal:greenplum://");
        dscPropers.setProperty("GREENPLUM_Driver", "com.pivotal.jdbc.GreenplumDriver");
        dscPropers.setProperty("GREENPLUM_TAIL", "/;DatabaseName,");
        dscPropers.setProperty("MEMSQL", "jdbc:mysql://");
        dscPropers.setProperty("MEMSQL_Driver", "com.mysql.jdbc.Driver");
        dscPropers.setProperty("MEMSQL_TAIL", "/");
        dscPropers.setProperty("RDP", "jdbc:rdp://");
        dscPropers.setProperty("RDP_Driver", "com.rapidsdata.jdbcdriver.Driver");
        dscPropers.setProperty("RDP_TAIL", "/");
        dscPropers.setProperty("SPARK", "jdbc:hive2://");
        dscPropers.setProperty("SPARK_Driver", "org.apache.hive.jdbc.HiveDriver");
        dscPropers.setProperty("SPARK_TAIL", "/");
    }
    private DatasourceConnectionFactory() {
        throw new IllegalStateException("DatasourceConnectionFactory Is Utility Class");
    }

    public static String getJDBCUrl(DatabaseConfig databaseConfig){
        if (databaseConfig.getUrl() != null)
            return databaseConfig.getUrl();
        StringBuilder url = new StringBuilder();
//        DataSourceType dataSourceType = databaseConfig.getDataSourceType();
        String dbType = databaseConfig.getDataSourceType().toString();
        if(DataSourceType.HUDI.toString().equals(dbType))
            dbType = "SPARK";
        url.append(dscPropers.getProperty(dbType));
        // get hostname、port      e.g:  192.168.1.1、3306
        if(databaseConfig.getHost() != null && databaseConfig.getPort() != null){
            url.append(databaseConfig.getHost());
            url.append(":");
            url.append(databaseConfig.getPort());
        }else
            throw new TCMException("Create JDBCUrl error,pls check host and port.");

        // Add different rules    e.g:   "/" or "//"
        url.append(dscPropers.getProperty(dbType + "_TAIL"));

        // get connection databaseName
        if (databaseConfig.getDatabasename()!=null)
            url.append(databaseConfig.getDatabasename());
        if(DataSourceType.MYSQL.toString().equals(dbType))
            url.append("?useSSL=false");

        return url.toString();
    }

    // use DatabaseConfig to create connection
    public static Connection createDataSourceConnection(DatabaseConfig databaseConfig){
        Driver driver = null;
        Properties properties = new Properties();
        // appending user
        properties.put("user",databaseConfig.getUsername());
        // appending password , def ""
        if(databaseConfig.getPassword() == null)
            properties.put("password","");
        else
            properties.put("password",databaseConfig.getPassword());

        DataSourceType dataSourceType = databaseConfig.getDataSourceType();

        String jdbcUrl = getJDBCUrl(databaseConfig);
        if (databaseConfig.getUrl() == null)
            databaseConfig.setUrl(jdbcUrl);

        // Get the load path of the driver
        try {
            Class<?> aClass = Class.forName(databaseConfig.getDriver() != null
                    ? databaseConfig.getDriver()
                    : dscPropers.getProperty(dataSourceType.toString() + "_Driver"));
            driver = (Driver)aClass.newInstance();
            return driver.connect(jdbcUrl,properties);
        }catch (ClassNotFoundException e){
            // in Class.forName()
            throw new TCMException("Create datasource connection fail,Please make sure to find the correct driver !!!",e);
        } catch (IllegalAccessException|InstantiationException e) {
            // in Class.newInstance()
            throw new TCMException("Create datasource connection fail,Please check if the driver can be initialized !!!",e);
        }catch (SQLException e){
            // in driver.connect
            throw new TCMException("Create datasource connection fail,Pass the information to the driver to obtain connection is fail !!!",e);
        }
    }

    // execute the SQL and return the result set
    public static List executeQuerySQL(DatabaseConfig databaseConfig, String sql){
        List list = new LinkedList();
        try(
                Connection conn = createDataSourceConnection(databaseConfig);
                Statement statement = conn.createStatement()
        ) {
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            System.out.println("columnCount:"+columnCount);
            while (rs.next()){
                Map rowData = new LinkedHashMap();
                for (int i = 1;i <= columnCount;i++)
                    rowData.put(md.getColumnName(i),rs.getString(i));
                list.add(rowData);
            }
            return list;
        }catch (SQLException e){
            throw new TCMException("executeQuery is fail, in "+databaseConfig.getUrl());
        }
    }


}
