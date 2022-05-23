package com.boraydata.cdc.tcm.utils;

import com.boraydata.cdc.tcm.exception.TCMException;

import java.io.BufferedWriter;
import java.io.File;
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

    /**
     *
     * @Param test.txt
     * @Return: txt
     */
    public static String getFileExtension(String path) {
        if(path == null || path.length() <= 0 )
            throw new TCMException("the file path is null,path:"+path);
        String extension = "";
        try  {
            File file = new File(path);
            if(Boolean.TRUE.equals(file.isDirectory()))
                throw new TCMException("this path is a Directory,path:"+path);
            if (file.exists()) {
                String name = file.getName();
                extension = name.substring(name.lastIndexOf(".")+1);
            }
        } catch (Exception e) {
            extension = "";
        }
        return extension;
    }

    // Check the path is directory
    public static boolean isDirectory(String path){
        File file = new File(path);
        return (file.isDirectory());
    }


    // write file , if shell exists , will delete .
    public static boolean writeMsgToFile(String msg, String path){
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
    /**
     * @see <a href="https://cloud.tencent.com/developer/article/1703463"></a>
     */
    public static boolean deleteFile(String path){
        File file = new File(path);
        if(file.exists()) {
            try {
                Files.delete(Paths.get(path));
            } catch (IOException e) {
//                e.printStackTrace();
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
