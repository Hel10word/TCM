package com.boraydata.cdc.tcm.entity;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * record table metadata information entity
 * @author bufan
 * @date 2021/8/25
 */
@JsonPropertyOrder({
        "dataSourceEnum",
//        "originalDataSourceEnum",
        "catalogName",
        "schemaName",
        "tableName",
        "primaryKeys",
        "columns",
})
@JsonIgnoreProperties({"originalDataSourceEnum",})
public class Table implements Cloneable {
    private DataSourceEnum dataSourceEnum;
    // When the table data source type be modified, this can record the original type.
    private DataSourceEnum originalDataSourceEnum;
    // Catalog
    private String catalogName;
    // Schema
    private String schemaName;
    private String tableName;
    private String primaryKeyName;
    private List<String> primaryKeys = new LinkedList<>();
    private List<Column> columns = new LinkedList<>();


    public DataSourceEnum getDataSourceEnum() {
        return dataSourceEnum;
    }

    public Table setDataSourceEnum(DataSourceEnum dataSourceEnum) {
        this.dataSourceEnum = dataSourceEnum;
        return this;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Table setCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Table setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public Table setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }

    public Table setPrimaryKeyName(String primaryKeyName) {
        this.primaryKeyName = primaryKeyName;
        return this;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Table setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Table setColumns(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public DataSourceEnum getOriginalDataSourceEnum() {
        return originalDataSourceEnum;
    }

    public Table setOriginalDataSourceEnum(DataSourceEnum originalDataSourceEnum) {
        this.originalDataSourceEnum = originalDataSourceEnum;
        return this;
    }

    public String outTableInfo(){
        StringBuilder info = new StringBuilder();
        info
                .append(outInfo()).append("\n")
//                .append("\tDataSourceEnum:").append(this.dataSourceEnum).append("\n")
//                .append("\tCatalogName:").append(this.catalogName).append("\n")
//                .append("\tSchemaName:").append(this.schemaName).append("\n")
//                .append("\tTableName:").append(this.tableName).append("\n")
                .append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ columns info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        for(Column column : columns)
            info.append("\t").append(column.outInfo());
        return info.toString();
    }

    public String outInfo(){
        return String.format("DataSourceEnum:%-20s OriginalDataSourceEnum:%-20s CatalogName:%-20s SchemaName:%-20s TableName:%-20s ColumnCount:%-20s PrimaryKeyName:%-20s PrimaryKeys:%-20s",
                                dataSourceEnum,     originalDataSourceEnum,     catalogName,      schemaName,       tableName,      columns.size(),   primaryKeyName,      primaryKeys);
    }

    @Override
    public String toString() {
        return this.outInfo();
    }

    public Table clone() {
        Table table = new Table();
        try {
            table = (Table) super.clone();
            List<Column> cloneList = new LinkedList<>();
            for(Column column : this.columns)
                cloneList.add(column.clone());
            table.setPrimaryKeyName(null);
            table.setPrimaryKeys(primaryKeys);
            table.setColumns(cloneList);
            table.setCatalogName(null);
            table.setSchemaName(null);
            table.setDataSourceEnum(null);
            table.setOriginalDataSourceEnum(originalDataSourceEnum);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return table;
    }
}
