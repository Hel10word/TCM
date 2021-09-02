package com.boraydata.tcm;

import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManage;

/** TCM- Table Clone Manager
 * @author bufan
 * @data 2021/8/25
 */
public class Test {

    public static void main(String[] args) {

        // 1、创建 Source 数据源信息
        DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
        DatabaseConfig sourceConfig = sourceBuilder
                .setDatabasename("test_db")
                .setDataSourceType(DataSourceType.MYSQL)
                .setHost("192.168.30.192")
                .setPort("3306")
                .setUsername("root")
                .setPassword("root")
                .create();

        //2、 创建 Clone 数据源信息
        DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
        DatabaseConfig cloneConfig = cloneBuilder
                .setDatabasename("test_db")
                .setDataSourceType(DataSourceType.POSTGRES)
                .setHost("192.168.30.192")
                .setPort("5432")
                .setUsername("postgres")
                .setPassword("")
                .create();

        //3、 创建管理器上下文 设置 相关信息
        TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
        TableCloneManageContext tcmc = tcmcBuilder
                .setSourceConfig(cloneConfig)
                .setCloneConfig(sourceConfig)
//                .setSourceConfig(sourceConfig)
//                .setCloneConfig(cloneConfig)
                .create();

        //4、 创建管理器
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmc);

        //5、 使用 tcm 通过 表名 来获取表数据
        Table sourceTable = tcm.getSourceTable("colume_type");

        //6、 将该表映射为 Clone Datasource 类型
        Table cloneTable = tcm.mappingCloneTable(sourceTable);

//        可以修改表名
        cloneTable.setTablename("colume_type_copy");

        // 7、将 该表在 Clone Datasource 上创建，并获得 执行结果
        boolean flag = tcm.createTableInCloneDatasource(cloneTable);
        if(flag)
            System.out.println("create table Success");
        else
            System.out.println("Create table Failure");
    }

}
