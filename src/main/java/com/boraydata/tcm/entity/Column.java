package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataTypeMapping;


/** 用来记录一些列的信息
 * @author bufan
 * @data 2021/8/25
 */
public class Column implements Cloneable {

    // Catalog
    private String TableCatalog;
    //表的 Schema
    private String TableSchema;
    // 表名
    private String TableName;
    // 字段名
    private String ColumnName;
    // 字段类型
    private String DataType;
    // 字段所在位置
    private Integer OrdinalPosition;
    // 字段是否允许为空
    private Boolean IsNullAble;
    // 映射到内部的字段
    private DataTypeMapping dataTypeMapping;

    public Column() {}

    public DataTypeMapping getDataTypeMapping() {
        return dataTypeMapping;
    }

    public String getColumnInfo(){
        return this.TableCatalog+"."+this.TableSchema+"."+this.TableName+"  col: "+this.ColumnName+"  type:"+this.DataType;
    }

    public Column setDataTypeMapping(DataTypeMapping dataTypeMapping) {
        this.dataTypeMapping = dataTypeMapping;
        return this;
    }

    public String getTableCatalog() {
        return TableCatalog;
    }

    public Column setTableCatalog(String tableCatalog) {
        TableCatalog = tableCatalog;
        return this;
    }

    public String getTableSchema() {
        return TableSchema;
    }

    public Column setTableSchema(String tableSchema) {
        TableSchema = tableSchema;
        return this;
    }

    public String getTableName() {
        return TableName;
    }

    public Column setTableName(String tableName) {
        TableName = tableName;
        return this;
    }

    public String getColumnName() {
        return ColumnName;
    }

    public Column setColumnName(String columnName) {
        ColumnName = columnName;
        return this;
    }

    public String getDataType() {
        return DataType;
    }

    public Column setDataType(String dataType) {
        DataType = dataType;
        return this;
    }

    public Integer getOrdinalPosition() {
        return OrdinalPosition;
    }

    public Column setOrdinalPosition(Integer ordinalPosition) {
        OrdinalPosition = ordinalPosition;
        return this;
    }

    public Boolean isNullAble() {
        return IsNullAble;
    }

    public Column setNullAble(Boolean nullAble) {
        IsNullAble = nullAble;
        return this;
    }

    @Override
    public String toString() {
        return "Column{" +
                "Catalog='" + TableCatalog + '\'' +
                ", Schema='" + TableSchema + '\'' +
                ", TableName='" + TableName + '\'' +
                ", ColumnName='" + ColumnName + '\'' +
                ", DataType='" + DataType + '\'' +
                ", Position=" + OrdinalPosition +
                ", IsNullAble=" + IsNullAble +
                ", dataTypeMapping=" + dataTypeMapping +
                '}';
    }

    public Column clone(){
        Column column = null;
        try {
            column = (Column) super.clone();
            column.setTableCatalog(null);
            column.setTableSchema(null);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return column;
    }
}
