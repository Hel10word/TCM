package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;

/** generate and execute the command
 * @author bufan
 * @data 2021/9/25
 */
public class CommandManage {
    DatabaseConfig sourceConfig;
    DatabaseConfig cloneConfig;
    String filePath;
    String tableName;
    CommandGenerate sourceCommand;
    CommandGenerate cloneCommand;


    public CommandManage(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,String filePath,String tableName){
        this.sourceConfig = sourceConfig;
        this.cloneConfig = cloneConfig;
        this.filePath = filePath+tableName+"_"+
                sourceConfig.getDataSourceType().toString()+"_to_"+
                cloneConfig.getDataSourceType().toString()+".csv";
        this.tableName = tableName;
        this.sourceCommand = CommandGenerateFactory.create(sourceConfig.getDataSourceType());
        this.cloneCommand = CommandGenerateFactory.create(cloneConfig.getDataSourceType());
    }
    private String getExport(DatabaseConfig config,String filePath,String tableName){
        return sourceCommand.exportCommand(config,filePath,tableName);
    }
    private String getLoad(DatabaseConfig config,String filePath,String tableName){
        return cloneCommand.exportCommand(config,filePath,tableName);
    }




}
