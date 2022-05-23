package com.boraydata.cdc.tcm.utils;

import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author bufan
 * @Description
 * @data 2022/5/18
 */
public class JacksonUtil {

    public static String toJson(Object o) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper.writeValueAsString(o);
    }
    public static <T> T stringToObject(String json,Class<T> tClass){
        try {
            return new ObjectMapper()
                    // 反序列化时 忽略 未知的属性
                    // 也可以在对象上 添加 注解
                    // @JsonIgnoreProperties(ignoreUnknown = true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readerFor(tClass)
                    .readValue(json);
        } catch (JsonProcessingException e) {
            throw new TCMException("the JSON format mismatch "+tClass.getTypeName(),e);
        }
    }

    public static <T> T filePathToObject(String filePath,Class<T> tClass){
        try(FileInputStream in = new FileInputStream(filePath);){
            return new ObjectMapper()
                    // 反序列化时 忽略 未知的属性
                    // 也可以在对象上 添加 注解
                    // @JsonIgnoreProperties(ignoreUnknown = true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readerFor(tClass)
                    .readValue(in);
        } catch (JsonProcessingException e) {
            throw new TCMException("the JSON format mismatch "+tClass.getTypeName(),e);
        } catch (FileNotFoundException e) {
            throw new TCMException("not found file",e);
        } catch (IOException e) {
            throw new TCMException("failed to read file",e);
        }
    }
}
