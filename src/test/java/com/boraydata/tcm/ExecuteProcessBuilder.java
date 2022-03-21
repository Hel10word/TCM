package com.boraydata.tcm;

import java.io.File;
import java.io.InputStream;

public class ExecuteProcessBuilder {

    public static void main(String[] args) throws InterruptedException{
        String dir = "",file_one  = "",file_two = "";
        if(args.length > 0)
            dir = args[0];
        if(args.length > 1)
            file_one = args[1];
        if(args.length > 2)
            file_two = args[2];
        ExecuteShellBuilder(dir,file_one);
        System.out.println("sleep 10 s");
        Thread.sleep(10000);
        ExecuteShellBuilder(dir,file_two);
    }
    public static void ExecuteShellBuilder(String dir, String shellScriptName){
        long start = System.currentTimeMillis();
        ProcessBuilder pb = new ProcessBuilder("sh",shellScriptName);
        pb.directory(new File(dir));
        StringBuffer sb = new StringBuffer(200);
        byte[] bytesPool = new byte[2048];
        try {
            Process p = pb.start();
            try (InputStream stdInput = p.getInputStream(); InputStream stdError = p.getErrorStream()) {
                String line;
                while (stdInput.read(bytesPool) != -1){
                    line = new String(bytesPool,"utf-8");
                    sb.append("\n stdInput:").append(line);
                }
                while (stdError.read(bytesPool) != -1){
                    line = new String(bytesPool,"utf-8");
                    sb.append("\n stdError:").append(line);
                }
                try {
                    int runningStatus = p.waitFor();
                }catch (InterruptedException e){
                }
                p.destroy();
                System.out.println(sb.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(dir+shellScriptName+" spend time: "+(System.currentTimeMillis()-start));
    }
}
