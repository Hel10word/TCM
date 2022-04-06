package com.boraydata.tcm.utils;

import com.boraydata.tcm.exception.TCMException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Check the file or directory exists, and write or delete file.
 * @author bufan
 * @data 2021/9/28
 */
public class FileUtil {

    // Check the path is directory
    public static boolean IsDirectory(String path){
        File file = new File(path);
        return (file.isDirectory());
    }

    public static boolean Mkdirs(String path){
        File file = new File(path);
        return file.mkdirs();
    }

    // write file , if shell exists , will delete .
    public static boolean WriteMsgToFile(String msg,String path){
        File file = createNewFile(path);
//        try(BufferedWriter br = new BufferedWriter(new FileWriter(file))){
        try(BufferedWriter br = Files.newBufferedWriter(
                Paths.get(path),
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE)){
            br.write(msg);
            br.flush();
        }catch (IOException e){
            throw new TCMException("write '"+msg+"' to '"+path+"' failed");
        }
        return true;
    }

    // delete file
    public static boolean DeleteFile(String path){
        File file = new File(path);
        if(file.exists()) {
            try {
                Files.delete(Paths.get(path));
            } catch (IOException e) {
                e.printStackTrace();
                throw new TCMException("Unable Delete Cache File:"+path,e);
            }
        }
        return true;
    }
    public static File getFile (String path){
        File file = new File(path);
        if (!file.exists())
            throw new TCMException("Unable Find File:"+path);

        return file;
    }

    public static File createNewFile (String path){
        File file = new File(path);
        try {
            if(file.exists())
                Files.delete(Paths.get(path));
            if (!file.getParentFile().exists())
                file.getParentFile().mkdirs();
            file.createNewFile();
        }catch (IOException e){
            throw new TCMException("Create '"+path+"' failed");
        }
        return file;
    }

}
