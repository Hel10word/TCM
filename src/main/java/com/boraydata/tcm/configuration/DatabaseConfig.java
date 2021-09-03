package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;

/** Used to define metadata related information,this info be used to create Connection.
 * @author bufan
 * @data 2021/8/25
 */
public class DatabaseConfig {

    // 主机名
    private String host;
    // 端口号
    private String port;
    // 登陆 用户名
    private String username;
    // 登陆 密码
    private String password;
    // 数据库的类型 以及相关信息
    private DataSourceType dataSourceType;
    // 数据库名称
    private String databasename;
    // 连接驱动的名称
    private String driver;
    // 连接的地址 （该地址可以手动设置，若没有设置，在获取 Connection 时 会根据前面的相关信息生成 URL 并赋值）
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
        private DataSourceType dataSourceType;
        private String databasename;
        private String driver;
        private String url;

        public Builder() {
            // Do nothing .
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

}
