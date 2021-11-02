package com.boraydata.tcm;

import com.boraydata.tcm.command.CommandManage;
import com.boraydata.tcm.command.CommandManageFactory;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

/** TCM - Table Clone Manage and Table Data Sync
 * @author bufan
 * @data 2021/9/28
 */
public class DoIt {

    private static String SOURCE_DATABASE_NAME;
    private static String SOURCE_DATA_TYPE;
    private static String SOURCE_HOST;
    private static String SOURCE_PORT;
    private static String SOURCE_USER;
    private static String SOURCE_PASSWORD;

    private static String CLONE_DATABASE_NAME;
    private static String CLONE_DATA_TYPE;
    private static String CLONE_HOST;
    private static String CLONE_PORT;
    private static String CLONE_USER;
    private static String CLONE_PASSWORD;

//    private static String TABLE_NAME;
    private static String TEMP_DIRECTORY;
    private static String SELECT_LIMIT;
    private static String DELIMITER;

    private static Boolean DEBUG;


    private static String SOURCE_TABLE;
    private static String CLONE_TABLE;



    private static Properties properties;
    private static long start,end = 0;

    private static void init(){

        SOURCE_DATABASE_NAME = getValue("sourceDatabaseName");
        SOURCE_DATA_TYPE = getValue("sourceDataType");
        SOURCE_HOST = getValue("sourceHost");
        SOURCE_PORT = getValue("sourcePort");
        SOURCE_USER = getValue("sourceUser");
        SOURCE_PASSWORD = getValue("sourcePassword");

        CLONE_DATABASE_NAME = getValue("cloneDatabaseName");
        CLONE_DATA_TYPE = getValue("cloneDataType");
        CLONE_HOST = getValue("cloneHost");
        CLONE_PORT = getValue("clonePort");
        CLONE_USER = getValue("cloneUser");
        CLONE_PASSWORD = getValue("clonePassword");

//        TABLE_NAME = getValue("tableName");
        TEMP_DIRECTORY = getValue("tempDirectory","./TCMTemp");
        SELECT_LIMIT = getValue("selectLimit","");
        DELIMITER = getValue("delimiter","|");

        DEBUG = "true".equals(getValue("debug",""));

        SOURCE_TABLE = getValue("sourceTable");
        CLONE_TABLE = getValue("cloneTable");


        // 'mysql', 'postgresql', 'hive'
        if(SOURCE_DATA_TYPE.equals("mysql"))
            SOURCE_DATA_TYPE = DataSourceType.MYSQL.toString();
        else if (SOURCE_DATA_TYPE.equals("postgresql"))
            SOURCE_DATA_TYPE = DataSourceType.POSTGRES.toString();

        if(CLONE_DATA_TYPE.equals("mysql"))
            CLONE_DATA_TYPE = DataSourceType.MYSQL.toString();
        else if (CLONE_DATA_TYPE.equals("postgresql"))
            CLONE_DATA_TYPE = DataSourceType.POSTGRES.toString();
    }

    private static String getValue(String key){
        return getValue(key,"");
    }
    private static String getValue(String key,String def){
        String value = properties.getProperty(key);
        if(value == null || value.length()<=0){
            System.out.println("The key: "+key+" is null,use defValue: '"+def+"'.");
            return def;
        }
        return value;
    }

    public static void main(String[] args) {
        String configFilePath = args[0];
        if(configFilePath == null)
            throw new TCMException("you should incoming properties File.");
        if(!new File(configFilePath).exists())
            throw new TCMException("the '"+configFilePath+"' properties File is not found.");

        try {
            properties = new Properties();
            //  use ClassLoader load properties file
            FileInputStream in = new FileInputStream(configFilePath);
            //  InputStream in = DoIt.class.getClassLoader().getResourceAsStream("./config.properties");
            //  use properties object load file inputStream
            properties.load(in);
        } catch (IOException e) {
            throw new TCMException("load the 'config.properties' is not found",e);
        }

        // read the properties file and initialize the information.
        init();


        // 1、创建 Source 数据源信息
        DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
        DatabaseConfig sourceConfig = sourceBuilder
                .setDatabasename(SOURCE_DATABASE_NAME)
                .setDataSourceType(DataSourceType.getTypeByStr(SOURCE_DATA_TYPE))
                .setHost(SOURCE_HOST)
                .setPort(SOURCE_PORT)
                .setUsername(SOURCE_USER)
                .setPassword(SOURCE_PASSWORD)
                .create();

        //2、 创建 Clone 数据源信息
        DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
        DatabaseConfig cloneConfig = cloneBuilder
                // you can also set URL instead of Databasename、Host、Port
                //.setUrl("jdbc:postgresql://192.168.30.192:5432/test_db")
                .setDatabasename(CLONE_DATABASE_NAME)
                .setDataSourceType(DataSourceType.getTypeByStr(CLONE_DATA_TYPE))
                .setHost(CLONE_HOST)
                .setPort(CLONE_PORT)
                .setUsername(CLONE_USER)
                .setPassword(CLONE_PASSWORD)
                .create();

        //3、 创建管理器上下文 设置 相关信息
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmContext = tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .create();

        if (DEBUG){
            System.out.println("====  sourceConfig  ====\n"+sourceConfig.getCofInfo());
            System.out.println("\n====  cloneConfig  ====\n"+cloneConfig.getCofInfo());
        }

        //4、 创建管理器
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);

        System.out.println("\n(1).Read to initialization table in "
                +CLONE_DATA_TYPE+" ,from "
                +SOURCE_DATA_TYPE+"."+SOURCE_TABLE);

        System.out.println("\n\t(1.1).get '"+SOURCE_TABLE+"' info from "+SOURCE_DATA_TYPE);
        start = System.currentTimeMillis();
        //5、 使用 tcm 通过 表名 来获取表数据
        Table sourceTable = tcm.getSourceTable(SOURCE_TABLE);

        if (DEBUG)
            sourceTable.getTableInfo();

        end = System.currentTimeMillis();
        System.out.println("\t----get table info total time spent:" + (end - start)+"\n");



        //6、 将该表映射为 Clone Datasource 类型
        Table cloneTable = tcm.mappingCloneTable(sourceTable,CLONE_TABLE);
        if (DEBUG)
            cloneTable.getTableInfo();

//        可以修改表名
//        cloneTable.setTablename("colume_type_copy");

        start = System.currentTimeMillis();
        System.out.println("\t(1.2).create '"+CLONE_TABLE+"' in "+CLONE_DATA_TYPE);
//       7、将 该表在 Clone Datasource 上创建，并获得 执行结果
        boolean flag = tcm.createTableInCloneDatasource(cloneTable,DEBUG);
        if(!flag)
            throw new TCMException("Create table Failure!!!!!");

        end = System.currentTimeMillis();
        System.out.println("\t----create table total time spent:" + (end - start)+"\n");


        System.out.println("----initialization table success !!!\n\n");

//=====================================  Table Data SYNC   =====================================

        System.out.println("(2).Read to Sync table data, export "
                +SOURCE_DATA_TYPE+"."+SOURCE_TABLE
                +" ,load to "+CLONE_DATA_TYPE+"."+CLONE_TABLE);


        start = System.currentTimeMillis();

        CommandManage commandManage = CommandManageFactory.create(sourceConfig,cloneConfig,TEMP_DIRECTORY,DELIMITER,SELECT_LIMIT);
        commandManage.syncTableDataByTableName(sourceTable,cloneTable);

        end = System.currentTimeMillis();
        System.out.println("\t----Sync table data total time spent:" + (end - start)+"\n");
        System.out.println("----Sync the table success !!!!");

    }
}
