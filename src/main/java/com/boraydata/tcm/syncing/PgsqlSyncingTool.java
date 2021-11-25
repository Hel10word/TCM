package com.boraydata.tcm.syncing;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;

/**  Export and Load Table Data in PgSQL,by shell statements.
 * @author bufan
 * @data 2021/9/26
 */
public class PgsqlSyncingTool implements SyncingTool {

    @Override
    public boolean exportFile(DatabaseConfig config, AttachConfig attachConfig) {
        String exportCommand =  exportCommand(config,attachConfig);
        String exportShellPath = attachConfig.getExportShellPath();

//        System.out.println(exportCommand);
//        return true;

        if(FileUtil.WriteMsgToFile(exportCommand,exportShellPath)){
            if(FileUtil.Exists(exportShellPath))
                return CommandExecutor.execuetShell(exportShellPath,attachConfig.getDebug());
        }
        return false;
    }

    @Override
    public boolean loadFile(DatabaseConfig config, AttachConfig attachConfig) {
        String loadCommand = loadCommand(config, attachConfig);
        String loadShellPath = attachConfig.getLoadShellPath();

//        System.out.println(loadCommand);
//        return true;

        if(FileUtil.WriteMsgToFile(loadCommand,loadShellPath)){
            if(FileUtil.Exists(loadShellPath))
                return CommandExecutor.execuetShell(loadShellPath,attachConfig.getDebug());
        }
        return false;
    }



    // -- export
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy (select * from lineitem_1 limit 5) to '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"
    //-- load
    //psql postgres://postgres:postgres@192.168.30.155/test_db -c "\copy lineitem from '/usr/local/lineitem_1_limit_5.csv' with DELIMITER ',';"

    private String getConnectCommand(DatabaseConfig config){
        return String.format("psql postgres://%s:%s@%s/%s -c  \"?\" 2>&1",
                config.getUsername(),
                config.getPassword(),
                config.getHost(),
                config.getDatabasename());
    }
    private String completeExportCommand(String com,String tableName,String filePath, String delimiter){
        return com.replace("?",
                "\\copy "+tableName+" to " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }
    private String completeLoadCommand(String com,String filePath, String tableName, String delimiter){
        return com.replace("?",
                "\\copy "+tableName+" from " +
                        "'"+filePath+"' with DELIMITER '"+delimiter+"';");
    }

    public String exportCommand(DatabaseConfig config,AttachConfig attachConfig) {
        if(!StringUtil.isNullOrEmpty(attachConfig.getTempTableSQL())){
            return completeExportCommand(getConnectCommand(config),"("+attachConfig.getTempTableSQL().replaceAll(";","")+")",attachConfig.getLocalCsvPath(),attachConfig.getDelimiter());
        }else
            return completeExportCommand(getConnectCommand(config),attachConfig.getSourceTableName(),attachConfig.getLocalCsvPath(),attachConfig.getDelimiter());
    }

    public String loadCommand(DatabaseConfig config,AttachConfig attachConfig) {
        return completeLoadCommand(getConnectCommand(config),attachConfig.getLocalCsvPath(),attachConfig.getCloneTableName(),attachConfig.getDelimiter());
    }

}
