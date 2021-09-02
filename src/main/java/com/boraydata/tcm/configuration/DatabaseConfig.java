package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;

/** Used to define metadata related information
 * @author bufan
 * @data 2021/8/25
 */
public class DatabaseConfig {

    private String host;
    private String port;
    private String username;
    private String password;
    // 数据库的类型 以及相关信息
    private DataSourceType dataSourceType;
    // 数据库名称
    private String databasename;
    // 连接驱动的名称
    private String driver;
    // 连接的地址
    private String url;


    DatabaseConfig(Builder builder){
        username = builder.username;
        password = builder.password;
        dataSourceType = builder.dataSourceType;
        url = builder.url;
        host = builder.host;
        port = builder.port;
        databasename = builder.databasename;
        driver = builder.driver;
    }

    public DatabaseConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public String getUrl() {
        return url;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getDatabasename() {
        return databasename;
    }

    public String getDriver() {
        return driver;
    }

    public static class Builder{
        private String host;
        private String port;
        private String username;
        private String password;
        // 数据库的类型
        private DataSourceType dataSourceType;
        // 数据库名称
        private String databasename;
        private String driver;
        // 根据以上信息 生成 连接 数据库的 URL
        private String url;

        public Builder() {
        }

        public DatabaseConfig create() {
            return new DatabaseConfig(this);
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }


        public Builder setDriver(String driver) {
            this.driver = driver;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(String port) {
            this.port = port;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDataSourceType(DataSourceType dataSourceType) {
            this.dataSourceType = dataSourceType;
            return this;
        }

        public Builder setDatabasename(String databasename) {
            this.databasename = databasename;
            return this;
        }
    }

//    @Override
//    public String toString() {
//        if (url != null)
//            return url+" username : "+username+" password : "+password;
//
//        return "DatabaseConfig{" +
//                "host='" + host + '\'' +
//                ", port='" + port + '\'' +
//                ", username='" + username + '\'' +
//                ", password='" + password + '\'' +
//                ", dataSourceType=" + dataSourceType.name() +
//                ", databasename='" + databasename + '\'' +
//                ", driver='" + driver + '\'' +
//                '}';
//    }
}
