package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataSourceType;

import java.util.LinkedList;
import java.util.List;

/**
 * @author bufan
 * @data 2021/8/25
 */
public class Table implements Cloneable {
    private DataSourceType dataSourceType;
    // Catalog
    private String catalogName;
    // Schema
    private String schemaName;
    private String tableName;
    // When the table data source type be modified, this can record the most original type.
    private DataSourceType sourceType;
    private List<Column> columns = new LinkedList<>();

    public Table() {
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public Table setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
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

    public List<Column> getColumns() {
        return columns;
    }

    public Table setColumns(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public DataSourceType getSourceType() {
        return sourceType==null?dataSourceType:sourceType;
    }

    public Table setSourceType(DataSourceType sourceType) {
        this.sourceType = sourceType;
        return this;
    }

    public String getTableInfo(){
        StringBuilder info = new StringBuilder();
        info
                .append("\tDataSourceType:").append(this.dataSourceType).append("\n")
                .append("\tCatalogName:").append(this.catalogName).append("\n")
                .append("\tSchemaName:").append(this.schemaName).append("\n")
                .append("\tTableName:").append(this.tableName).append("\n")
                .append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ columns info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        for(Column column : columns)
            info.append("\t").append(column.getInfo());
        return info.toString();
    }

    @Override
    public String toString() {
        return "Table{" +
                "dataSourceType=" + dataSourceType +
                ", catalogname='" + catalogName + '\'' +
                ", schemaname='" + schemaName + '\'' +
                ", tablename='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }

    public Table clone() {
        Table table = new Table();
        try {
            table = (Table) super.clone();
            List<Column> cloneList = new LinkedList<>();
            for(Column column : this.columns)
                cloneList.add(column.clone());
            table.setColumns(cloneList);
            table.setCatalogName(null);
            table.setSchemaName(null);
            table.setDataSourceType(null);
            table.setSourceType(sourceType);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return table;
    }
}
