package com.boraydata.cdc.tcm.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * @author bufan
 * @date 2021/9/1
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


    @Test
    void escapeStrTest() {
        System.out.println(StringUtil.escapeRegexQuoteEncode("\n"));
        char ball = (char) 7;
        String ball_str = String.valueOf(ball);
        System.out.println(ball);
        System.out.println(ball_str);
        String oct = "\011";
        String hex = "\u0011";
        System.out.println(oct);
        System.out.println(hex);

    }


}