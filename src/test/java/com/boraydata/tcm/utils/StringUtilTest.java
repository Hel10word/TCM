package com.boraydata.tcm.utils;

import com.boraydata.tcm.core.DataSourceType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author bufan
 * @data 2021/9/1
 */
class StringUtilTest {

    @Test
    public void DataTypeFormatTest() {
        String[] strArray = {"Bit(1)","bit(232)","bits(232)","bit(255,0,0)","BIT(12)"};
        for (String s : strArray)
            System.out.println(s+" --> "+StringUtil.dataTypeFormat(s));
    }

    @Test
    public void testSearchArray(){
        System.out.println(Arrays.binarySearch(new String[]{"asd","Asd"},"Asd"));
    }


//    @Test
//    public void EqualsIgnoreCaseTest() {
//        String key = "bit(1)";
//        String[] strArray = {"BIT(1)","Bit(1)","bIT(1)"};
//        for (String s : strArray)
//            System.out.println(key+" = "+s+" --> "+StringUtil.equalsIgnoreCase(key,s));
//    }


}