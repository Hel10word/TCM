package com.boraydata.tcm.core;

/**
 * @author bufan
 * @data 2021/8/31
 */
class DataTypeMappingTest {

    public static void main(String[] args) {
        System.out.println(TableCloneManageType.BOOLEAN);
        System.out.println(TableCloneManageType.BOOLEAN.name());
        System.out.println(TableCloneManageType.BOOLEAN.value);
        System.out.println(TableCloneManageType.BOOLEAN.getOutDataType(DataSourceType.MYSQL));
        System.out.println(TableCloneManageType.BOOLEAN.getOutDataType(DataSourceType.POSTGRES));
    }

}