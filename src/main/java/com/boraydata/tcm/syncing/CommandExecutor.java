package com.boraydata.tcm.syncing;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author bufan
 * @data 2021/11/4
 */
public class CommandExecutor {
    public static boolean execuetShell(String shellPath,boolean outFlag){
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command("/bin/sh",shellPath);
            Process start = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(start.getInputStream()))) {
                String line;
                if(Boolean.TRUE.equals(outFlag))
                    System.out.println("\t  Excuet Shell : "+shellPath);
                while ((line = reader.readLine()) != null){
                    if(Boolean.TRUE.equals(outFlag))
                        System.out.println("\t Shell Out:"+line);
                }
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
