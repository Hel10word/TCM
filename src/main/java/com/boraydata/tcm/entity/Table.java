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
    private String catalogname;
    // Schema
    private String schemaname;
    private String tablename;
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

    public String getCatalogname() {
        return catalogname;
    }

    public Table setCatalogname(String catalogname) {
        this.catalogname = catalogname;
        return this;
    }

    public String getSchemaname() {
        return schemaname;
    }

    public Table setSchemaname(String schemaname) {
        this.schemaname = schemaname;
        return this;
    }

    public String getTablename() {
        return tablename;
    }

    public Table setTablename(String tablename) {
        this.tablename = tablename;
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
                .append("\tCatalogName:").append(this.catalogname).append("\n")
                .append("\tSchemaName:").append(this.schemaname).append("\n")
                .append("\tTableName:").append(this.tablename).append("\n")
                .append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ columns info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        for(Column column : columns)
            info.append("\t").append(column.getInfo());
        return info.toString();
    }

    @Override
    public String toString() {
        return "Table{" +
                "dataSourceType=" + dataSourceType +
                ", catalogname='" + catalogname + '\'' +
                ", schemaname='" + schemaname + '\'' +
                ", tablename='" + tablename + '\'' +
                ", columns=" + columns +
                '}';
    }

    public Table clone() {
        Table table = null;
        try {
            table = (Table) super.clone();
            List<Column> cloneList = new LinkedList<>();
            for(Column column : this.columns)
                cloneList.add(column.clone());
            table.setColumns(cloneList);
            table.setCatalogname(null);
            table.setSchemaname(null);
            table.setSourceType(dataSourceType);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return table;
    }
}
