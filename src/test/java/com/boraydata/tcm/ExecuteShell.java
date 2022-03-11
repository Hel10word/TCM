package com.boraydata.tcm;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author bufan
 * @data 2022/2/27
 */
public class ExecuteShell {



    public static void main(String[] args) throws InterruptedException, IOException {
        String dir = "",file_one  = "",file_two = "";
        if(args.length > 0)
            dir = args[0];
        if(args.length > 1)
            file_one = args[1];
        if(args.length > 2)
            file_two = args[2];

        ExecuteShellBuilder(dir,file_one);

//        ExecuteShellRuntime(dir,file_one);
//
        System.out.println("sleep 10 s");
        Thread.sleep(10000);
//
//        ExecuteShellRuntime(dir,file_two);

        ExecuteShellBuilder(dir,file_two);

//        Thread thread_1 = new ExecuteShellThread(dir,file_one);
//        thread_1.start();
//
//        Thread.sleep(10000);
//
//        if(file_two != null && file_two.length()>0) {
//            Thread thread_2 = new ExecuteShellThread(dir,file_two);
//            thread_2.start();
//        }
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
//                    System.out.println("stdInput:"+line);
                        sb.append("\n stdInput:").append(line);
                    }
                    while (stdError.read(bytesPool) != -1){
                        line = new String(bytesPool,"utf-8");
//                    System.out.println("stdError:"+line);
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
            System.out.println(shellScriptName+" spend time: "+(System.currentTimeMillis()-start));
        }
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
//                    System.out.println("stdInput:"+line);
                    sb.append("\n stdInput:").append(line);
                }
                while (stdError.read(bytesPool) != -1){
                    line = new String(bytesPool,"utf-8");
//                    System.out.println("stdError:"+line);
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
        System.out.println(shellScriptName+" spend time: "+(System.currentTimeMillis()-start));
    }

    public static void ExecuteShellRuntime(String dir, String shellScriptName) throws IOException {
        long start = System.currentTimeMillis();
        String []cmdArray = new String[]{"/bin/sh",shellScriptName};
        Runtime runtime = Runtime.getRuntime();
        Process p = runtime.exec(cmdArray);
        StringBuffer sb = new StringBuffer(200);
        byte[] bytesPool = new byte[2048];
        try {
            try (InputStream stdInput = p.getInputStream(); InputStream stdError = p.getErrorStream()) {
                String line;
                while (stdInput.read(bytesPool) != -1){
                    line = new String(bytesPool,"utf-8");
//                    System.out.println("stdInput:"+line);
                    sb.append("\n stdInput:").append(line);
                }
                while (stdError.read(bytesPool) != -1){
                    line = new String(bytesPool,"utf-8");
//                    System.out.println("stdError:"+line);
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
        System.out.println(shellScriptName+" spend time: "+(System.currentTimeMillis()-start));
    }
}





//    private static class StreamGobbler implements Runnable {
//        private InputStream inputStream;
//        private Consumer<String> consumer;
//
//        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
//            this.inputStream = inputStream;
//            this.consumer = consumer;
//        }
//
//        @Override
//        public void run() {
//            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
//        }
//    }

//    public static String ExecutorShells(String dir, String shellScriptName, boolean outFlag){
//        ProcessBuilder pb = new ProcessBuilder();
//        pb.command("sh",shellScriptName);
//        pb.directory(new File(dir));
//        StringBuffer sb = new StringBuffer(200);
//        int runningStatus = 0;
//        try {
//            Process p = pb.start();
//            try (BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
//                 BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
//                String line;
//                while ((line = stdInput.readLine()) != null)
//                    sb.append("\n stdInput:").append(line).append("\n");
//                while ((line = stdError.readLine()) != null)
//                    sb.append("\n stdInput:").append(line).append("\n");
//                try {
//                    runningStatus = p.waitFor();
//                }catch (InterruptedException e){
//                }
//                p.destroy();
//                if(Boolean.TRUE.equals(outFlag))
//                    return sb.toString();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
