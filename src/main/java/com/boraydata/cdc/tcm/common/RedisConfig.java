package com.boraydata.cdc.tcm.common;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author bufan
 * @data 2022/4/6
 */
@JsonPropertyOrder({
        "host",
        "port",
        "auth",
        "messageKey",
        "expireSecond",
})
public class RedisConfig {

    private String host;
    private String port;
    private String auth;
    private String messageKey;
    private Integer expireSecond = 86_400;

    public String getHost() {
        return host;
    }

    public RedisConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public String getPort() {
        return port;
    }

    public RedisConfig setPort(String port) {
        this.port = port;
        return this;
    }

    public String getAuth() {
        return auth;
    }

    public RedisConfig setAuth(String auth) {
        this.auth = auth;
        return this;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public RedisConfig setMessageKey(String messageKey) {
        this.messageKey = messageKey;
        return this;
    }

    public Integer getExpireSecond() {
        return expireSecond;
    }

    public RedisConfig setExpireSecond(Integer expireSecond) {
        this.expireSecond = expireSecond;
        return this;
    }
}
