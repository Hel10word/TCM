package com.boraydata.cdc.tcm.common;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Objects;

/**
 * define metadata connection info.
 * @author bufan
 * @date 2021/8/25
 */
@JsonPropertyOrder({
        "dataSourceEnum",
        "host",
        "port",
        "username",
        "password",
        "databaseName",
        "catalog",
        "schema",
        "driver",
        "jdbcUrl",
})
public class DatabaseConfig {
    private DataSourceEnum dataSourceEnum;
    private String host;
    private String port;
    private String username;
    private String password;
    private String databaseName;
    private String catalog;
    private String schema;
    private String driver;
    private String jdbcUrl;

    public DatabaseConfig checkConfig(){
        switch (dataSourceEnum){
            case MYSQL:
                if(StringUtil.isNullOrEmpty(this.catalog))
                    this.catalog = "def";
                if(StringUtil.isNullOrEmpty(this.schema))
                    this.schema = this.databaseName;
                if(StringUtil.isNullOrEmpty(this.port))
                    this.port = "3306";
                break;
            case POSTGRESQL:
                if(StringUtil.isNullOrEmpty(databaseName) && !StringUtil.isNullOrEmpty(catalog))
                    this.databaseName = this.catalog;
                String[] split_postgreSQL = this.databaseName.split("\\.");
                if(split_postgreSQL.length == 2){
                    this.catalog = split_postgreSQL[0];
                    this.schema = split_postgreSQL[1];
                    this.databaseName = split_postgreSQL[0];
                }
                if(StringUtil.isNullOrEmpty(this.catalog))
                    this.catalog = this.databaseName;
                if(StringUtil.isNullOrEmpty(this.schema))
                    this.schema = "public";
                if(StringUtil.isNullOrEmpty(this.port))
                    this.port = "5432";
                break;
            case SQLSERVER:
                if(StringUtil.isNullOrEmpty(databaseName) && !StringUtil.isNullOrEmpty(catalog))
                    this.databaseName = this.catalog;
                String[] split_sqlserver = this.databaseName.split("\\.");
                if(split_sqlserver.length == 2){
                    this.catalog = split_sqlserver[0];
                    this.schema = split_sqlserver[1];
                    this.databaseName = split_sqlserver[0];
                }
                if(StringUtil.isNullOrEmpty(this.catalog))
                    this.catalog = this.databaseName;
                if(StringUtil.isNullOrEmpty(this.schema))
                    this.schema = "dbo";
                if(StringUtil.isNullOrEmpty(this.port))
                    this.port = "1433";
                break;
            default:
                return this;
        }

        if(StringUtil.isNullOrEmpty(databaseName))
            throw new TCMException("the database name is null"+this.outInfo());
        return this;
    }

    public DataSourceEnum getDataSourceEnum() {
        return dataSourceEnum;
    }

    public DatabaseConfig setDataSourceEnum(DataSourceEnum dataSourceEnum) {
        this.dataSourceEnum = dataSourceEnum;
        return this;
    }
    @JsonSetter("dataSourceEnum")
    public DatabaseConfig setDataSourceEnum(String str) {
        this.dataSourceEnum = DataSourceEnum.valueOfByString(str);
        return this;
    }

    public String getHost() {
        return host;
    }

    public DatabaseConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public String getPort() {
        return port;
    }

    public DatabaseConfig setPort(String port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public DatabaseConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseConfig setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public DatabaseConfig setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public DatabaseConfig setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getDriver() {
        return driver;
    }

    public DatabaseConfig setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public DatabaseConfig setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }



    public String outInfo(){
        return  "\thost:"+host+"\n"+
                "\tport:"+port+"\n"+
                "\tusername:"+username+"\n"+
                "\tpassword:"+password+"\n"+
                "\tdataSourceEnum:"+dataSourceEnum.toString()+"\n"+
                "\tdatabaseName:"+databaseName+"\n"+
                "\tcatalog:"+catalog+"\n"+
                "\tschema:"+schema+"\n"+
                "\tdriver:"+driver+"\n"+
                "\tjdbcUrl:"+jdbcUrl+"\n";
    }

}
