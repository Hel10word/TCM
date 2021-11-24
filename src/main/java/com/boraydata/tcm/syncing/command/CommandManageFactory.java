//package com.boraydata.tcm.command;
//
//import com.boraydata.tcm.configuration.DatabaseConfig;
//import com.boraydata.tcm.core.DataSourceType;
//import com.boraydata.tcm.exception.TCMException;
//import com.boraydata.tcm.utils.FileUtil;
//
//import java.io.File;
//
///** to create *CommandGenerate and CommandManage
// * @author bufan
// * @data 2021/9/28
// */
//public class CommandManageFactory {
//
//    public static CommandManage create(DatabaseConfig sourceConfig,DatabaseConfig cloneConfig,String dir,String delimiter,String selectLimit){
//        if(!FileUtil.Exists(dir)&&!FileUtil.Mkdirs(dir)&&!FileUtil.IsDirectory(dir))
//            throw new TCMException("the '"+dir+"' is not exist,you should make sure the Directory Path can reach.");
//        CommandGenerate sourceCom = SyncingToolFactory.create(sourceConfig.getDataSourceType());
//        CommandGenerate cloneCom = SyncingToolFactory.create(cloneConfig.getDataSourceType());
//        return new CommandManage(sourceConfig,cloneConfig,sourceCom,cloneCom,dir,delimiter,selectLimit);
//    }
//}
