package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.exception.TCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

/**
 * @author bufan
 * @date 2021/11/4
 */
public class CommandExecutor {
    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);
    public static String executeShell(String dir,String shellScriptName, boolean outFlag){
        log.info("Execute Shell Path:{}{}",dir,shellScriptName);
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("sh",shellScriptName);
        pb.directory(new File(dir));
        StringBuffer sb = new StringBuffer(200);
        byte[] bytesPool = new byte[2048];
        int runningStatus = 0;
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
                    runningStatus = p.waitFor();
                }catch (InterruptedException e){
                    throw new TCMException("Failed to Execute Shell by executeShell -> "+dir+shellScriptName,e);
                }
                p.destroy();
                stdInput.close();
                stdError.close();
                if(Boolean.TRUE.equals(outFlag))
                    return sb.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
