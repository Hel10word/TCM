package com.boraydata.cdc.tcm.syncing.execute;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

/**
 * @author bufan
 * @date 2022/3/15
 */
public class ExecuteProcess {

    public static void main(String[] args) throws InterruptedException, IOException {
        String dir = "",file_one  = "",file_two = "";
        if(args.length > 0)
            dir = args[0];
        if(args.length > 1)
            file_one = args[1];
        if(args.length > 2)
            file_two = args[2];
        ExecuteShellRuntime(dir,file_one);
        System.out.println("sleep 10 s");
        Thread.sleep(10000);
        ExecuteShellRuntime(dir,file_two);
    }

    public static void ExecuteShellRuntime(String dir, String shellScriptName) throws IOException {
        long start = System.currentTimeMillis();
        String []cmdArray = new String[]{"/bin/sh", Paths.get(dir, shellScriptName).toString()};
        Runtime runtime = Runtime.getRuntime();
        Process p = runtime.exec(cmdArray);
        StringBuffer sb = new StringBuffer(200);
        byte[] bytesPool = new byte[2048];
        try {
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
