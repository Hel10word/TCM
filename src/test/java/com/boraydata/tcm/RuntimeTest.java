package com.boraydata.tcm;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author bufan
 * @data 2021/9/23
 */
public class RuntimeTest {

    @Test
    public void runtimeShellTest(){
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec("ping baidu.com", null, null);
            InputStream stdeer = proc.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdeer, "GBK");
            BufferedReader br = new BufferedReader(isr);
            String line = "";
            while ((line = br.readLine()) != null){
                System.out.println(line);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printResults(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        System.out.println(" something out");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }
}
