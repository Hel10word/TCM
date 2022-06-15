package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Table;

/**
 * @author bufan
 * @date 2021/8/30
 */
public interface MappingTool {

    /**
     * Complete original column data type mapping to TCMDataTypeEnum
     *  To look up mapping relation between {@link TCMDataTypeEnum} and OriginalDataType by the ‘mappingMap’
     * @Param: table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=null}}
     * @Return: table={...,..,..,columns={...,...,...,DataType=integer,dataTypeMapping=INT32}}
     */
    Table createSourceMappingTable(Table table);
    /**
     * According to column TCMDataTypeEnum create a clone table
     * return a clone table,and set DataType by {@link TCMDataTypeEnum}
     * @Param: table={...,POSTGRESQL,..,columns={...,...,col_int,DataType=serial,dataTypeMapping=INT32}}
     * @Return: table={...,MYSQL,..,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     */
    Table createCloneMappingTable(Table table);
    // According to the table generate SQL Statement
    String getCreateTableSQL(Table table);

}
