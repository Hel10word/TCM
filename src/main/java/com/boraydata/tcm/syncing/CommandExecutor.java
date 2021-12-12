package com.boraydata.tcm.syncing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author bufan
 * @data 2021/11/4
 */
public class CommandExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CommandExecutor.class);
    public static boolean executeShell(String shellPath, boolean outFlag){
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command("/bin/sh",shellPath);
            Process start = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(start.getInputStream()))) {
                String line;
                logger.info("\t\t\t!!!!!!!!!!!!!!!!!!!!!!!!!! Shell Path :{} !!!!!!!!!!!!!!!!!!!!!!!!!!!",shellPath);
                while ((line = reader.readLine()) != null){
                    if(Boolean.TRUE.equals(outFlag))
                        logger.info("\t\t\tShell Shell :{}",line);
                }
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
