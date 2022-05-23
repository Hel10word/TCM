package com.boraydata.cdc.tcm.syncing.execute;

import java.io.*;

/**
 * @author bufan
 * @data 2021/9/23
 *
 * refer:
 * https://blog.csdn.net/whatday/article/details/107082238
 */

public class CommandExecuetTest {

    // E:\environment\javaJDK\jdk1.8.0_144\bin
    static String envp[] = {"Path=E:\\environment\\javaJDK\\jdk1.8.0_144;E:\\environment\\javaJDK\\jdk1.8.0_144\\bin;E:\\environment\\javaJDK\\jdk1.8.0_144\\jre\\bin;"};

    public static void main(String[] args) {
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec("cmd /c path && java -version 2>&1", envp, new File("D://"));
//            Process proc = rt.exec("powershell pwd 2>&1", null, null);

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
//            String envp[] = {"Path=E:\\environment\\javaJDK\\jdk1.8.0_144;E:\\environment\\javaJDK\\jdk1.8.0_144\\bin;E:\\environment\\javaJDK\\jdk1.8.0_144\\jre\\bin;"};
//            Process proc = rt.exec("cmd /c path && java -version 2>&1", envp, new File("D://"));
//            Process proc = rt.exec("powershell pwd 2>&1", null, null);
/**
 *   exec方法第一个参数是执行的命令，第二个参数是环境变量，第三个参数是工作目录
 *
 *  如果不指定 会 采用 系统默认的环境变量，及为 cmd.exe 加载的换进变量
 */
