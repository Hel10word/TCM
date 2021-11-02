package com.boraydata.tcm.command;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.FileUtil;

import java.io.File;

/** to create *CommandGenerate and CommandManage
 * @author bufan
 * @data 2021/9/28
 */
public class CommandManageFactory {

    public static CommandManage create(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,String dir,String delimiter,String selectLimit){
        if(!FileUtil.Exists(dir)&&!FileUtil.Mkdirs(dir)&&!FileUtil.IsDirectory(dir))
            throw new TCMException("the '"+dir+"' is not exist,you should make sure the Directory Path can reach.");
        CommandGenerate sourceCom = getCommandGenerate(sourceConfig.getDataSourceType());
        CommandGenerate cloneCom = getCommandGenerate(cloneConfig.getDataSourceType());
        return new CommandManage(sourceConfig,cloneConfig,sourceCom,cloneCom,dir,delimiter,selectLimit);
    }

    private static CommandGenerate getCommandGenerate(DataSourceType type){
        if (type == DataSourceType.MYSQL)
            return new MysqlCommandGenerate();
        else if (type == DataSourceType.POSTGRES)
            return new PgsqlCommandGenerate();
        else if (type == DataSourceType.SPARK)
            return new SparkCommandGenerate();
        return null;
    }
}
