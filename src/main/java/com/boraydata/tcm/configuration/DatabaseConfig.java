package com.boraydata.tcm.configuration;

import com.boraydata.tcm.core.DataSourceType;

/** define metadata connection info.
 * @author bufan
 * @data 2021/8/25
 */
public class DatabaseConfig {

    private String host;
    private String port;
    private String username;
    private String password;
    private DataSourceType dataSourceType;
    private String databasename;
    private String driver;
    private String url;
    // default tableName
    private String tableName;

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

    public String getTableName() {
        return tableName;
    }

    public DatabaseConfig setTableName(String tableName) {
        this.tableName = tableName;
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


    public String getCofInfo(){
        return  "\thost:"+host+"\n"+
                "\tport:"+port+"\n"+
                "\tusername:'"+username+"'\n"+
                "\tpassword:'"+password+"'\n"+
                "\tdataSourceType:"+dataSourceType.toString()+"\n"+
                "\tdatabasename:"+databasename+"\n"+
                "\ttableName:"+tableName+"\n"+
                "\turl:"+url+"\n";
    }

}
