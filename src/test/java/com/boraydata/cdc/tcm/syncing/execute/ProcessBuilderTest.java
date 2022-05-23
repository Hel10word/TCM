package com.boraydata.cdc.tcm.syncing.execute;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author bufan
 * @data 2021/9/27
 */
public class ProcessBuilderTest {
    public static void main(String[] args) throws IOException {
        //        System.out.println(System.getenv());
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("cmd","/c","path");
        Process start = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(start.getInputStream(),"GBK"))) {
            String line;
            while ((line = reader.readLine()) != null)
                System.out.println(line);
        }
    }
}
