package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataSourceType;

import java.util.LinkedList;
import java.util.List;

/** 存储 表的信息
 * @author bufan
 * @data 2021/8/25
 */
public class Table implements Cloneable {
    // 数据库类型
    private DataSourceType dataSourceType;
    // Catalog
    private String catalogname;
    // Schema
    private String schemaname;
    // 表名
    private String tablename;
    // 表下面列的信息
    private List<Column> columns = new LinkedList<>();

    public Table() {
    }

    public Table(DataSourceType dataSourceType, String catalogname, String schemaname, String tablename, List<Column> columns) {
        this.dataSourceType = dataSourceType;
        this.catalogname = catalogname;
        this.schemaname = schemaname;
        this.tablename = tablename;
        this.columns = columns;
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
            List<Column> cloneList = new LinkedList<>();;
            for(Column column : columns)
                cloneList.add((Column) column.clone());
            table.setColumns(cloneList);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return table;
    }
}