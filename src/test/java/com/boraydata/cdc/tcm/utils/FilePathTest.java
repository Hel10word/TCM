package com.boraydata.cdc.tcm.utils;

import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * @author bufan
 * @date 2021/11/9
 */
public class FilePathTest {
    @Test
    public void buildUpPath(){
        System.out.println(File.separator);
        System.out.println(new File("./TCM-Temp","Faile").getPath());
        System.out.println(new File("./TCM-Temp/","Faile").getPath());
        System.out.println(new File("./TCM-Temp/","/Faile").getPath());
        System.out.println(new File("./TCM-Temp//test","/Faile").getPath());

    }
}
