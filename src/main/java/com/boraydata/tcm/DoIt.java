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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/** TCM - Table Clone Manage and Table Data Sync
 * @author bufan
 * @data 2021/9/28
 */
public class DoIt {

    private static Properties properties;
    private static long start,end = 0;

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
    private static DataSourceType getTypeByStr(String str){
        if(str.equals(DataSourceType.MYSQL.toString()))
            return DataSourceType.MYSQL;
        else if(str.equals(DataSourceType.POSTGRES.toString()))
            return DataSourceType.POSTGRES;
        return null;
    }







    public static void main(String[] args) {
        try {
            properties = new Properties();
            // use ClassLoader load properties file
            FileInputStream in = new FileInputStream("./config.properties");
//            InputStream in = DoIt.class.getClassLoader().getResourceAsStream("./config.properties");
            // use properties object load file inputStream
            properties.load(in);
        } catch (IOException e) {
            throw new TCMException("load the 'config.properties' is not found",e);
        }


        // 1、创建 Source 数据源信息
        DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
        DatabaseConfig sourceConfig = sourceBuilder
                .setDatabasename(getValue("sourceDatabaseName"))
                .setDataSourceType(getTypeByStr(getValue("sourceDataType")))
                .setHost(getValue("sourceHost"))
                .setPort(getValue("sourcePort"))
                .setUsername(getValue("sourceUser"))
                .setPassword(getValue("sourcePassword"))
                .create();

        //2、 创建 Clone 数据源信息
        DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
        DatabaseConfig cloneConfig = cloneBuilder
                // you can also set URL instead of Databasename、Host、Port
                //.setUrl("jdbc:postgresql://192.168.30.192:5432/test_db")
                .setDatabasename(getValue("cloneDatabaseName"))
                .setDataSourceType(getTypeByStr(getValue("cloneDataType")))
                .setHost(getValue("cloneHost"))
                .setPort(getValue("clonePort"))
                .setUsername(getValue("cloneUser"))
                .setPassword(getValue("clonePassword"))
                .create();

        //3、 创建管理器上下文 设置 相关信息
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmContext = tcmcBuilder
                .setSourceConfig(sourceConfig)
                .setCloneConfig(cloneConfig)
                .create();

        //4、 创建管理器
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);

        System.out.println("\n(1).Read to initialization table in "
                +getValue("cloneDataType")+" ,from "
                +getValue("sourceDataType")+"."+getValue("tableName"));

        String TableName = getValue("tableName");

        System.out.println("\n\t(1.1).get '"+TableName+"' info from "+getValue("sourceDataType"));
        start = System.currentTimeMillis();
        //5、 使用 tcm 通过 表名 来获取表数据
        Table sourceTable = tcm.getSourceTable(TableName);
        end = System.currentTimeMillis();
        System.out.println("\t----get table info total time spent:" + (end - start)+"\n");


        //6、 将该表映射为 Clone Datasource 类型
        Table cloneTable = tcm.mappingCloneTable(sourceTable);

//        可以修改表名
//        cloneTable.setTablename("colume_type_copy");

        start = System.currentTimeMillis();
        System.out.println("\t(1.2).create '"+TableName+"' in "+getValue("cloneDataType"));
//       7、将 该表在 Clone Datasource 上创建，并获得 执行结果
        boolean flag = tcm.createTableInCloneDatasource(cloneTable);
        if(!flag)
            throw new TCMException("Create table Failure!!!!!");

        end = System.currentTimeMillis();
        System.out.println("\t----create table total time spent:" + (end - start)+"\n");


        System.out.println("----initialization table success !!!\n\n");

//=====================================  Table Data SYNC   =====================================

        System.out.println("(2).Read to Sync table data, export "
                +getValue("sourceDataType")+"."+getValue("tableName")
                +" ,load to "+getValue("cloneDataType"));

        String dir = getValue("tempDirectory");
        String delimiter = ",";
        if (getValue("delimiter") != null)
            delimiter = getValue("delimiter");
        String selectLimit = ""+getValue("selectLimit");
        start = System.currentTimeMillis();

        CommandManage commandManage = CommandManageFactory.create(sourceConfig,cloneConfig,dir,delimiter,selectLimit);
        commandManage.syncTableDataByTableName(TableName);


        end = System.currentTimeMillis();
        System.out.println("\t----Sync table data total time spent:" + (end - start)+"\n");
        System.out.println("----Sync the table success !!!!");


    }
}
