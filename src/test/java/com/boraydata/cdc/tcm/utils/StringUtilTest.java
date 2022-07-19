package com.boraydata.cdc.tcm.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.StringEscapeUtils;
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
        char ball = (char) 7;
        String ball_str = String.valueOf(ball);
        System.out.println(ball);
        System.out.println(ball_str);
        String oct = "\011";
        String hex1 = "\uF011";
        System.out.println(oct);
        System.out.println(Integer.valueOf('\11'));
    }
    @Test
    void name() {
        System.out.println(String.valueOf((char) 7));
        System.out.println("test");
    }


    @Test
    void name2() throws JsonProcessingException {

        System.out.println(escapeJava("\""));
        System.out.println(escapeJava("\\"));
        System.out.println(escapeJava("\n"));
        System.out.println(escapeJava("\\'"));
        System.out.println(escapeJava("|\n"));
        System.out.println("\n\n----\n\n");

        String test = "this \n is'quo \" teest";
//        System.out.println(StringUtil.escapeRegexDoubleQuoteEncode(test));

//        System.out.println("\""+ escapeDoubleQuote(test)+"\"");
//        System.out.println("'"+ escapeSingleQuote(test)+"'");


        String key_d = StringEscapeUtils.escapeJava("\"");
        String key_s = StringEscapeUtils.escapeJava("'");
        String escape_key = StringEscapeUtils.escapeJava("\\");


        System.out.println(test);
        System.out.println(StringEscapeUtils.escapeJava(test));
        System.out.println(escapeJava(test));

        System.out.println("\n\n-----\n\n");
        System.out.println(escapeJava(test,"\"","\\"));
        System.out.println(escapeJava(test,"'","\\"));
        System.out.println(escapeJava(test,"\\n","\\"));
        System.out.println(escapeJava(test,escapeJava("\n"),"\\"));
    }


    @Test
    void name3() {
        String tableName = "table";
        String escape = "\\";
        String delimiter = ",";
        String line = "\n";
        String quote = "\"";


        String command = "";
        tableName = escapeJava(tableName);
        escape = escapeJava(escape);
        delimiter = escapeJava(delimiter);
        line = escapeJava(line);
        quote = escapeJava(quote);
        System.out.println("\ntableName:"+tableName+"\nescape:"+escape+"\ndelimiter:"+delimiter+"\nline:"+line+"\nquote:"+quote);
        command = "tableName:'"+escapeJava(tableName,"'")+
                "' -e escape:\""+escapeJava(escape,"\"")+"\" "+
                "delimiter:\""+escapeJava(delimiter,"\"")+"\" "+
                "line:\""+escapeJava(line,"\"")+"\" "+
                "quote:\""+escapeJava(quote,"\"")+"\" ";
        System.out.println(command);
        System.out.println("\""+escapeJava(command,"\"")+"\"");
        System.out.println("'"+escapeJava(command,"'")+"'");




    }

    String escapeJava(String str){
        if(null == str)
            return "";
        else
            return StringEscapeUtils.escapeJava(str)
//                "\"" => """
                .replaceAll("\\\\\"","\"");
//                "\\" => "\"
//                .replaceAll("\\\\\\\\","\\\\");
    }

    String escapeJava(String str,String key){
        if(null == str)
            return "";
        else
            return escapeJava(str,key,"\\");
    }

    String escapeJava(String str,String key,String escape_key){
        if(null == str)
            return "";
        else
            return str.replaceAll(StringEscapeUtils.escapeJava(key),
                    StringEscapeUtils.escapeJava(escape_key)+StringEscapeUtils.escapeJava(key));
    }

    String escapeDoubleQuote(String str){
        if(null == str)
            return "";
        else
            return escapeJava(str).replaceAll("\"","\\\\\"");
    }
    String escapeSingleQuote(String str){
        if(null == str)
            return "";
        else
            return escapeJava(str).replaceAll("'","\\\\'");
    }
}