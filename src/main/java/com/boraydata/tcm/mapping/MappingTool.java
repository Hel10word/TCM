package com.boraydata.tcm.mapping;

import com.boraydata.tcm.entity.Table;

/** declare to create mapping relationship table
 * @author bufan
 * @data 2021/8/30
 */
public interface MappingTool {

    // Complete the Table.columns.DataTypeMapping
    Table createSourceMappingTable(Table table);
    // Create a new mapping table
    Table createCloneMappingTable(Table table);
    // According to the table generate SQL
    String getCreateTableSQL(Table table);

}
