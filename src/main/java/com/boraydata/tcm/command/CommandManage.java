package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.FileUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/** generate and execute the command
 * @author bufan
 * @data 2021/9/25
 */
public class CommandManage {
    DatabaseConfig sourceConfig;
    DatabaseConfig cloneConfig;
    String sourceDB;
    String cloneDB;
    String dir;
    String delimiter;
    String limit;
    CommandGenerate sourceCommand;
    CommandGenerate cloneCommand;


    public CommandManage(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,CommandGenerate sourceCommand,CommandGenerate cloneCommand,String dir,String delimiter,String limit){
        this.sourceConfig = sourceConfig;
        this.cloneConfig = cloneConfig;
        this.dir = dir;
        this.delimiter = delimiter;
        this.sourceCommand = sourceCommand;
        this.cloneCommand = cloneCommand;
        this.limit = limit;

        this.sourceDB = sourceConfig.getDataSourceType().toString();
        this.cloneDB = cloneConfig.getDataSourceType().toString();

        if(!FileUtil.Exists(dir) || !FileUtil.IsDirectory(dir))
            throw new TCMException("you should make sure Path='"+dir+"' is a directory and can reach.");
    }


    // sync the table data
    public void syncTableDataByTableName(String tableName){
        // e.g. : /usr/local/TableName_MSQL_to_PGSQL.csv
        String csvFilePath = this.dir+tableName+"_"+sourceDB+"_to_"+cloneDB+".csv";

        // e.g. : /usr/local/MSQL_Export_TableName.sh
        String exportShellPath = this.dir+sourceConfig.getDataSourceType().toString()+"_Export_"+tableName+".sh";

        // e.g. : /usr/local/PGSQL_Load_TableName.sh
        String loadShellPath = this.dir+cloneConfig.getDataSourceType().toString()+"_Load_"+tableName+".sh";


        String exportShell = sourceCommand.exportCommand(sourceConfig, csvFilePath, tableName, delimiter,limit);
        String loadShell = cloneCommand.loadCommand(cloneConfig, csvFilePath, tableName, delimiter);

        if(!FileUtil.WriteMsgToFile(exportShell, exportShellPath))
            throw new TCMException("create export shell failed");

        if(!FileUtil.WriteMsgToFile(loadShell, loadShellPath))
            throw new TCMException("create load shell failed");

        if(FileUtil.Exists(csvFilePath))
            FileUtil.DeleteFile(csvFilePath);
        System.out.println("\n\t(2.1).Create script files success.\n\t\t Export:'"+exportShellPath+"' \n\t\tLoad:'"+loadShellPath+"'");

        Long start,end;

        start = System.currentTimeMillis();
        System.out.println("\n\t(2.2).Read to Execute Export Shell,Export Table Data in '"+csvFilePath+"' from "+sourceConfig.getDataSourceType().toString());
        if(!execuetShell(exportShellPath))
            throw new TCMException("excuet export shell failed");
        end = System.currentTimeMillis();
        System.out.println("\t----Export CSV file total time spent:" + (end - start)+"\n\n");

        start = System.currentTimeMillis();
        System.out.println("\n\t(2.3).Read to Execute Load Shell,Load Table Data in "+cloneConfig.getDataSourceType().toString()+" from '"+csvFilePath+"'.");
        if(!execuetShell(loadShellPath))
            throw new TCMException("excuet load shell failed");
        end = System.currentTimeMillis();
        System.out.println("\t----Load CSV file total time spent:" + (end - start)+"\n\n");
    }

    private boolean execuetShell(String shellPath){
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command("/bin/sh",shellPath);
            Process start = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(start.getInputStream()))) {
                String line;
                System.out.println("\t  Excuet Shell : "+shellPath);
                while ((line = reader.readLine()) != null)
                    System.out.println("\t Shell Out:"+line);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
