package com.boraydata.tcm.syncing.execute;

import java.io.File;
import java.io.InputStream;

/**
 * @author bufan
 * @data 2022/3/15
 */
public class ExecuteThread {
    public static void main(String[] args) throws InterruptedException{
        String dir = "",file_one  = "",file_two = "";
        if(args.length > 0)
            dir = args[0];
        if(args.length > 1)
            file_one = args[1];
        if(args.length > 2)
            file_two = args[2];


        Thread thread_1 = new ExecuteShellThread(dir,file_one);
        thread_1.start();

        thread_1.join();
        System.out.println("sleep 10 s");
        Thread.sleep(10000);

        if(file_two != null && file_two.length()>0) {
            Thread thread_2 = new ExecuteShellThread(dir,file_two);
            thread_2.start();
        }
    }


    public static class ExecuteShellThread extends Thread{
        private String dir;
        private String shellScriptName;
        public ExecuteShellThread(String dir, String shellScriptName){
            this.dir = dir;
            this.shellScriptName = shellScriptName;
        }

        @Override
        public void run() {
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
}
