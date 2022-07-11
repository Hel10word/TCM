package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.exception.TCMException;

import java.sql.*;
import java.util.*;

/**
 * create database connection objects
 * @author bufan
 * @date 2021/8/25
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
        // Use mysql-driver-8, can connect mysql-5 and mysql-8.
        dscPropers.setProperty("MYSQL", "jdbc:mysql://");
        dscPropers.setProperty("MYSQL_Driver", "com.mysql.cj.jdbc.Driver");
//        dscPropers.setProperty("MYSQL_Driver", "com.mysql.jdbc.Driver");
        dscPropers.setProperty("MYSQL_TAIL", "/");
        dscPropers.setProperty("POSTGRESQL", "jdbc:postgresql://");
        dscPropers.setProperty("POSTGRESQL_Driver", "org.postgresql.Driver");
        dscPropers.setProperty("POSTGRESQL_TAIL", "/");

        dscPropers.setProperty("SQLSERVER", "jdbc:sqlserver://");
        dscPropers.setProperty("SQLSERVER_Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dscPropers.setProperty("SQLSERVER_TAIL", ";");

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
        dscPropers.setProperty("HIVE", "jdbc:hive2://");
        dscPropers.setProperty("HIVE_Driver", "org.apache.hive.jdbc.HiveDriver");
        dscPropers.setProperty("HIVE_TAIL", "/");
    }
    private DatasourceConnectionFactory() {
        throw new IllegalStateException("DatasourceConnectionFactory Is Utility Class");
    }
    private static String getDBType(DataSourceEnum dataSourceEnum){
        // Hudi、Spark、Hive default use Hive_Driver
        if(DataSourceEnum.HUDI.equals(dataSourceEnum))
            return "HIVE";
        if(DataSourceEnum.RPDSQL.equals(dataSourceEnum))
            return "MYSQL";
        return dataSourceEnum.name();
    }

    public static String getJDBCUrl(DatabaseConfig databaseConfig){
        if (databaseConfig.getJdbcUrl() != null)
            return databaseConfig.getJdbcUrl();
        StringBuilder url = new StringBuilder();
        String dbType = getDBType(databaseConfig.getDataSourceEnum());

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
        String databaseName = databaseConfig.getDatabaseName();
        if (databaseName != null) {
            if(DataSourceEnum.SQLSERVER.toString().equals(dbType)){
                /**
                 * @see <a href="https://docs.microsoft.com/en-us/sql/connect/jdbc/connecting-with-ssl-encryption?view=sql-server-linux-ver15"></a>
                 */
//                url.append("databaseName=").append(databaseName).append(";integratedSecurity=true;encrypt=true;trustServerCertificate=true");
                url.append("databaseName=").append(databaseName).append(";");
            }else {
                url.append(databaseName);
            }
            if (DataSourceEnum.MYSQL.toString().equals(dbType) || DataSourceEnum.RPDSQL.toString().equals(dbType))
//            url.append("?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT");
                url.append("?useSSL=false&serverTimezone=UTC&allowLoadLocalInfile=true");
        }
        return url.toString();
    }

    // use DatabaseConfig to create connection
    public static Connection createDataSourceConnection(DatabaseConfig databaseConfig){
        String dbType = getDBType(databaseConfig.getDataSourceEnum());

        Driver driver = null;
        Properties properties = new Properties();
        // appending user
        properties.put("user",databaseConfig.getUsername());
        // appending password , def ""
        if(databaseConfig.getPassword() == null)
            properties.put("password","");
        else
            properties.put("password",databaseConfig.getPassword());


        String jdbcUrl = getJDBCUrl(databaseConfig);

        String driverName = databaseConfig.getDriver() != null
                ? databaseConfig.getDriver()
                : dscPropers.getProperty(dbType + "_Driver");

        if (databaseConfig.getJdbcUrl() == null)
            databaseConfig.setJdbcUrl(jdbcUrl);
        if (databaseConfig.getDriver() == null)
            databaseConfig.setDriver(driverName);
        // Get the load path of the driver
        try {
            Class<?> aClass = Class.forName(driverName);
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
            throw new TCMException("Create datasource connection fail,Pass the information to the driver to obtain connection is fail !!!" +
                    "\ndriverName:"+driverName+"   JDBC-Url:"+jdbcUrl+"  properties:"+properties.toString(),e);
        }
    }

    // execute the SQL and return the result set
    public static List executeQuerySQL(DatabaseConfig databaseConfig, String sql){
        List list = new LinkedList();
        try(
                Connection conn = createDataSourceConnection(databaseConfig);
                Statement statement = conn.createStatement()
        ) {
//            boolean execute = statement.execute(sql);
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
            throw new TCMException("executeQuery is fail, in "+databaseConfig.getJdbcUrl());
        }
    }

/**
 * In MySQL connection query such as "select id as 'test_id' from example"
 * use getColumnName => id
 * use getColumnLabel => test_id
 * so can add "useOldAliasMetadataBehavior=true" in JDBC url or use getColumnLabel()
 * @see <a href="https://dev.mysql.com/doc/connectors/en/connector-j-connp-props-jdbc-compliance.html"></a>
 * @see <a href="https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-installing-upgrading-5-1.html"></a>
 */
    public static void showQueryBySQL(DatabaseConfig config, String sql){
        try(Connection con = DatasourceConnectionFactory.createDataSourceConnection(config);
            PreparedStatement ps = con.prepareStatement(sql)){
            ResultSet resultSet = ps.executeQuery();
            StringBuilder sb = new StringBuilder("Show Query By SQL:\n" + sql + "\n");
            while (resultSet.next()){
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int count = rsmd.getColumnCount();
                for (int i = 1;i <= count;i++){
//                    String columnName = rsmd.getColumnName(i);
                    String columnName = rsmd.getColumnLabel(i);
                    sb.append(String.format("%s:%-20s",columnName,resultSet.getString(columnName)));
                }
                sb.append("\n");
            }
            System.out.println(sb.toString());
        }catch (SQLException e){
            e.printStackTrace();
        }
    }


}
