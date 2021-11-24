//package com.boraydata.tcm.db2hive;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//
///**
// * TODO
// *
// * @date: 2020/7/31
// * @author: hatter
// **/
//public class SparkInputStreamReader implements Runnable {
//
//    private String name;
//    private BufferedReader reader;
//
//    private final Logger log = LoggerFactory.getLogger(SparkInputStreamReader.class);
//
//    public SparkInputStreamReader(InputStream is, String name) {
//        this.name = name;
//        this.reader = new BufferedReader(new InputStreamReader(is));
//        System.out.println("InputStreamReaderRunnable:  name=" + name);
//    }
//
//    @Override
//    public void run() {
//        try {
//            String line = reader.readLine();
//            while (line != null) {
//                System.out.println(line);
//                line = reader.readLine();
//            }
//        }
//        catch (Exception e) {
//            log.error("run() failed. for name="+ name);
//            log.error(e.getMessage());
//        }
//        finally {
//            try {
//                reader.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}