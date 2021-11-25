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
    // 记录上一个
    private DataSourceType sourceType;
    // 表下面列的信息
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

    public void outTableInfo(){
        System.out.println("DataSourceType:"+this.dataSourceType+"\n"
        +"CatalogName:"+this.catalogname+"\n"
        +"SchemaName:"+this.schemaname+"\n"
        +"TableName:"+this.tablename+"\n========== columns info ============="
        );
        for(Column column : columns)
            column.outInfo();
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
            table.setSourceType(dataSourceType);
            table.setCatalogname(null);
            table.setSchemaname(null);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return table;
    }
}
