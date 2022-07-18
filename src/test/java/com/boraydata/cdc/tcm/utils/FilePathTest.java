package com.boraydata.cdc.tcm.utils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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

    @Test
    void getPath() throws IOException {
        // E:\SoftWare\A_WORK\IDEA-workspace\boraydata-demo\TableCloneManage ./src

        String path = "./src";
        File file = new File(path);
        System.out.println("getAbsolutePath:"+file.getAbsolutePath());
        System.out.println("getCanonicalPath:"+file.getCanonicalPath());
        System.out.println("Paths.toAbsolutePath:"+Paths.get(path).toAbsolutePath().normalize().toString());
    }
}
