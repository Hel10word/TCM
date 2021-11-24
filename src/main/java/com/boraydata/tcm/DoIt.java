package com.boraydata.tcm;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** TCM - Table Clone Manage and Table Data Sync
 * @author bufan
 * @data 2021/9/28
 */
public class DoIt {

//    private static final Logger log = LoggerFactory.getLogger(DoIt.class);

    public static void main(String[] args)  {
        Properties properties;
        AttachConfig config;
        long start = 0;
        long end = 0;
        long all = 0;

        String configFilePath;
        if(args == null || args.length <= 0)
//            configFilePath = "./config.properties";
            throw new TCMException("you should incoming properties File.");
        else
            configFilePath = args[0];

        if(!new File(configFilePath).exists())
            throw new TCMException("the '"+configFilePath+"' properties File is not found.");
        try(FileInputStream in = new FileInputStream(configFilePath)){
//        try(InputStream in = DoIt.class.getClassLoader().getResourceAsStream("./config.properties")){
            properties = new Properties();
            properties.load(in);
            // read the properties file and initialize the information.
            config = AttachConfig.getInstance();
            // 1、读取并加载配置文件
            config.loadLocalConfig(properties);
        } catch (IOException e) {
            throw new TCMException("the '"+configFilePath+"' is not found",e);
        }
        String sourceTableName = config.getSourceTableName();
        String sourceDataType = config.getSourceDataType();
        String cloneTableName = config.getCloneTableName();
        String cloneDataType = config.getCloneDataType();


        // 2、创建 Source 数据源信息
        DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
        DatabaseConfig sourceConfig = sourceBuilder
                .setDatabasename(config.getSourceDatabaseName())
                .setDataSourceType(DataSourceType.getTypeByStr(config.getSourceDataType()))
                .setHost(config.getSourceHost())
                .setPort(config.getSourcePort())
                .setUsername(config.getSourceUser())
                .setPassword(config.getSourcePassword())
                .create();

        // 3、创建 Clone 数据源信息
        DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
        DatabaseConfig cloneConfig = cloneBuilder
                .setDatabasename(config.getCloneDatabaseName())
                .setDataSourceType(DataSourceType.getTypeByStr(config.getCloneDataType()))
                .setHost(config.getCloneHost())
                .setPort(config.getClonePort())
                .setUsername(config.getCloneUser())
                .setPassword(config.getClonePassword())
                .create();

        String sourceJdbcUrl = DatasourceConnectionFactory.getJDBCUrl(sourceConfig);
        String cloneJdbcUrl = DatasourceConnectionFactory.getJDBCUrl(cloneConfig);
        sourceConfig.setUrl(sourceJdbcUrl);
        cloneConfig.setUrl(cloneJdbcUrl);
        config.setSourceJdbcConnect(sourceJdbcUrl).setCloneJdbcConnect(cloneJdbcUrl);

        //4、 创建管理器上下文 设置 相关信息
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmContext = tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .setAttachConfig(config)
                .create();

                                    if (Boolean.TRUE.equals(config.getDebug())){
                                        System.out.println("====  sourceConfig  ====\n"+sourceConfig.getCofInfo());
                                        System.out.println("====  cloneConfig  ====\n"+cloneConfig.getCofInfo());
                                    }

        //5、 创建管理器
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);


        //======================================================== getSourceMappingTable ====================================================================
                                    System.out.println("\n(1).Read to initialization table in "
                                            +cloneDataType+" ,from "
                                            +sourceDataType+"."+sourceTableName);
                                    System.out.println("\n\t(1.1).get '"+sourceTableName+"' info from "+sourceDataType);
                                    start = System.currentTimeMillis();
        //6、 使用 tcm 通过 表名 来获取表数据
        Table sourceTable = tcm.createSourceMappingTable(sourceTableName);
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
                                    System.out.println("\t----get sourceTable info total time spent:" + (end - start)+"\n");
                                    if (Boolean.TRUE.equals(config.getDebug()))
                                        sourceTable.outTableInfo();


        //======================================================== getCloneTable ====================================================================

                                    start = System.currentTimeMillis();
                                    System.out.println("\n\t(1.2).create '"+cloneTableName+"' in "+cloneDataType);
        //7、 将该表映射为 Clone Datasource 类型
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneTableName);
        //8、将该表在 Clone Datasource 上创建，并获得 执行结果
        if(!tcm.createTableInDatasource())
            throw new TCMException("Create table Failure!!!!!");
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
                                    System.out.println("\t----create table total time spent:" + (end - start)+"\n");
                                    if (Boolean.TRUE.equals(config.getDebug()))
                                        cloneTable.outTableInfo();
                                    System.out.println("----initialization table success !!!\n\n");

//=====================================  Table Data SYNC   =====================================

                                    System.out.println("(2).Read to Sync table data, export "
                                            +sourceDataType+"."+sourceTableName
                                            +" ,load to "+cloneDataType+"."+cloneTableName+"\n");
                                    start = System.currentTimeMillis();
        //9、将 source 端的表导出
        if(!tcm.exportTableData())
            throw new TCMException("export table data Failure!!!!!");
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
                                    System.out.println("\t----export table data total time spent:" + (end - start)+"\n");


                                    start = System.currentTimeMillis();
        //10、在 Clone 端上导入表
        if(!tcm.loadTableData())
            throw new TCMException("load table data Failure!!!!!");
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
                                    System.out.println("\t----load table data total time spent:" + (end - start)+"\n");
                                    System.out.println("----Sync the table success !!!!");
                                    System.out.println("Mapping Table and Syncing TableData table time:"+all);
    }




}
