package com.boraydata.cdc.tcm.utils;

import com.boraydata.cdc.tcm.common.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Objects;

/**
 * @author bufan
 * @data 2022/1/7
 */
public class RedisOperateUtil {

    private static Jedis initJedis(RedisConfig config) {
        Jedis jedis = null;
        String host = config.getHost();
        String port = config.getPort();
        String auth = config.getAuth();
        if(!StringUtil.isNullOrEmpty(host) && !StringUtil.isNullOrEmpty(port))
            jedis = new Jedis(host,Integer.parseInt(port));
        if(jedis != null && !StringUtil.isNullOrEmpty(auth))
            jedis.auth(auth);
        return jedis;
    }

    public static String sendMessage(RedisConfig config, String value){
        if(Objects.isNull(config))
            return null;
        Jedis jedis = initJedis(config);
        String messageKey = config.getMessageKey();
        int expireSecond = config.getExpireSecond();

        if(jedis != null)
            jedis.set(messageKey,value,new SetParams().ex(expireSecond));
        else
            return String.format("Failed to write Redis(%s:%s)",config.getHost(),config.getPort());
        return String.format("Set String {key:%s\tvalue:%s\tTTL:%s} in Redis(%s:%s)",messageKey,value,config.getExpireSecond(),config.getHost(),config.getPort());
    }
}
