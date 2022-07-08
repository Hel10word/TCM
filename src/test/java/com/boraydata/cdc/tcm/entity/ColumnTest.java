package com.boraydata.cdc.tcm.entity;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import org.junit.jupiter.api.Test;

/**
 * @author bufan
 * @date 2021/11/3
 */
class ColumnTest {

    @Test
    public void cloneTest(){
        Column column = new Column();
        column.setDataSourceEnum(DataSourceEnum.MYSQL);
        column.setTcmDataTypeEnum(TCMDataTypeEnum.STRING);
        column.setTableName("testOne");

        Column clone = column.clone();

        System.out.println("DataSourceEnum    column:"+column.getDataSourceEnum()+"    clone:"+clone.getDataSourceEnum()+"  "+(column.getDataSourceEnum()==clone.getDataSourceEnum()));
        System.out.println("TCMDataTypeEnum    column:"+column.getTcmDataTypeEnum()+"    clone:"+clone.getTcmDataTypeEnum()+"  "+(column.getTcmDataTypeEnum()==clone.getTcmDataTypeEnum()));
        System.out.println("TableName    column:"+column.getTableName()+"    clone:"+clone.getTableName()+"  "+(column.getTableName() == clone.getTableName()));

        System.out.println("\n\n=========\n\n");
//        column.setDataSourceEnum(DataSourceEnum.SPARK);
        column.setTcmDataTypeEnum(TCMDataTypeEnum.BOOLEAN);
        column.setTableName("testTwo");

        System.out.println("DataSourceEnum    column:"+column.getDataSourceEnum()+"    clone:"+clone.getDataSourceEnum()+"  "+(column.getDataSourceEnum()==clone.getDataSourceEnum()));
        System.out.println("TCMDataTypeEnum    column:"+column.getTcmDataTypeEnum()+"    clone:"+clone.getTcmDataTypeEnum()+"  "+(column.getTcmDataTypeEnum()==clone.getTcmDataTypeEnum()));
        System.out.println("TableName    column:"+column.getTableName()+"    clone:"+clone.getTableName()+"  "+(column.getTableName()==clone.getTableName()));
    }

    @Test
    public void outInfoTest(){
        Column column = new Column();
        System.out.println(column.outInfo());
    }


}