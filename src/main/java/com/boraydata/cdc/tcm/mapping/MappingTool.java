package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.entity.Table;

/**
 * @author bufan
 * @data 2021/8/30
 */
public interface MappingTool {

    // Complete original column data type mapping to TCMDataTypeEnum
    Table createSourceMappingTable(Table table);
    // According to column TCMDataTypeEnum create a clone table
    Table createCloneMappingTable(Table table);
    // According to the table generate SQL Statement
    String getCreateTableSQL(Table table);

}
