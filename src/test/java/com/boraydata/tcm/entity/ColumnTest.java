package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @data 2021/11/3
 */
class ColumnTest {

    @Test
    public void cloneTest(){
        Column column = new Column();
        column.setDataSourceType(DataSourceType.MYSQL);
        column.setTableCloneManageType(TableCloneManageType.STRING);
        column.setTableName("testOne");

        Column clone = column.clone();

        System.out.println("DataSourceType    column:"+column.getDataSourceType()+"    clone:"+clone.getDataSourceType()+"  "+(column.getDataSourceType()==clone.getDataSourceType()));
        System.out.println("TableCloneManageType    column:"+column.getTableCloneManageType()+"    clone:"+clone.getTableCloneManageType()+"  "+(column.getTableCloneManageType()==clone.getTableCloneManageType()));
        System.out.println("TableName    column:"+column.getTableName()+"    clone:"+clone.getTableName()+"  "+(column.getTableName() == clone.getTableName()));

        System.out.println("\n\n=========\n\n");
//        column.setDataSourceType(DataSourceType.SPARK);
        column.setTableCloneManageType(TableCloneManageType.BOOLEAN);
        column.setTableName("testTwo");

        System.out.println("DataSourceType    column:"+column.getDataSourceType()+"    clone:"+clone.getDataSourceType()+"  "+(column.getDataSourceType()==clone.getDataSourceType()));
        System.out.println("TableCloneManageType    column:"+column.getTableCloneManageType()+"    clone:"+clone.getTableCloneManageType()+"  "+(column.getTableCloneManageType()==clone.getTableCloneManageType()));
        System.out.println("TableName    column:"+column.getTableName()+"    clone:"+clone.getTableName()+"  "+(column.getTableName()==clone.getTableName()));
    }

    @Test
    public void outInfoTest(){
        Column column = new Column();
        System.out.println(column.getColumnInfo());
    }


}