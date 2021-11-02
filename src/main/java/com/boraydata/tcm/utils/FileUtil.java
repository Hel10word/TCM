package com.boraydata.tcm.utils;

import com.boraydata.tcm.exception.TCMException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

    // Check the file exists
    public static boolean Exists(String path){
        File file = new File(path);
        return file.exists();
    }

    public static boolean Mkdirs(String path){
        File file = new File(path);
        return file.mkdirs();
    }

    // write file , if shell exists , will delete .
    public static boolean WriteMsgToFile(String msg,String path){
        File file = new File(path);
        try {
            if(Exists(path))
                file.delete();
            file.createNewFile();
        }catch (IOException e){
            throw new TCMException("Create '"+path+"' failed");
        }
        try(BufferedWriter br = new BufferedWriter(new FileWriter(file))){
            br.write(msg);

        }catch (IOException e){
            throw new TCMException("write '"+msg+"' to '"+path+"' failed");
        }
        return true;
    }

    // delete file
    public static boolean DeleteFile(String path){
        File file = new File(path);
        return file.delete();
    }



}
