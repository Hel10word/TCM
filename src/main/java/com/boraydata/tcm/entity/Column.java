package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;


/**
 * @author bufan
 * @data 2021/8/25
 */
public class Column implements Cloneable {
    // 数据库类型
    private DataSourceType dataSourceType;
    // Catalog
    private String TableCatalog;
    // Schema
    private String TableSchema;
    private String TableName;
    private String ColumnName;
    private String DataType;
    // standard data type
    private String UdtType;
    private Integer OrdinalPosition;
    // Char data type length
    private Long CharMaxLength;
    // numeric data type length
    private Integer NumericPrecisionM;
    // numeric data type precision
    private Integer NumericPrecisionD;
    // time data type precision
    private Integer DatetimePrecision;
    private Boolean IsNullAble;
    // mapping tcm data type
    private TableCloneManageType tableCloneManageType;

    public Column() {}

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public Column setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
        return this;
    }

    public TableCloneManageType getTableCloneManageType() {
        return tableCloneManageType;
    }

    public String getColumnInfo(){
        return this.TableCatalog+"."+this.TableSchema+"."+this.TableName+"  col: "+this.ColumnName+"  type:"+this.DataType;
    }

    public Column setTableCloneManageType(TableCloneManageType tableCloneManageType) {
        this.tableCloneManageType = tableCloneManageType;
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

    public String getUdtType() {
        return UdtType;
    }

    public Column setUdtType(String udtType) {
        UdtType = udtType;
        return this;
    }

    public Integer getOrdinalPosition() {
        return OrdinalPosition;
    }

    public Column setOrdinalPosition(Integer ordinalPosition) {
        OrdinalPosition = ordinalPosition;
        return this;
    }

    public Boolean getNullAble() {
        return IsNullAble;
    }

    public Column setNullAble(Boolean nullAble) {
        IsNullAble = nullAble;
        return this;
    }

    public Long getCharMaxLength() {
        return CharMaxLength;
    }

    public Column setCharMaxLength(Long charMaxLength) {
        CharMaxLength = charMaxLength;
        return this;
    }

    public Integer getNumericPrecisionM() {
        return NumericPrecisionM;
    }

    public Column setNumericPrecisionM(Integer numericPrecisionM) {
        NumericPrecisionM = numericPrecisionM;
        return this;
    }

    public Integer getNumericPrecisionD() {
        return NumericPrecisionD;
    }

    public Column setNumericPrecisionD(Integer numericPrecisionD) {
        NumericPrecisionD = numericPrecisionD;
        return this;
    }

    public Integer getDatetimePrecision() {
        return DatetimePrecision;
    }

    public Column setDatetimePrecision(Integer datetimePrecision) {
        DatetimePrecision = datetimePrecision;
        return this;
    }


    @Override
    public String toString() {
        return "Column{" +
                "dataSourceType=" + dataSourceType +
                ", TableCatalog='" + TableCatalog + '\'' +
                ", TableSchema='" + TableSchema + '\'' +
                ", TableName='" + TableName + '\'' +
                ", ColumnName='" + ColumnName + '\'' +
                ", DataType='" + DataType + '\'' +
                ", UdtType='" + UdtType + '\'' +
                ", OrdinalPosition=" + OrdinalPosition +
                ", CharMaxLength=" + CharMaxLength +
                ", NumericPrecisionM=" + NumericPrecisionM +
                ", NumericPrecisionD=" + NumericPrecisionD +
                ", DatetimePrecision=" + DatetimePrecision +
                ", IsNullAble=" + IsNullAble +
                ", tableCloneManageType=" + tableCloneManageType +
                '}';
    }

    public String getInfo(){
        return String.format("ColumnName=%-20s DataType=%-20s UdtType=%-20s Position=%-4d CharMaxLength=%-10d NumericPrecisionM=%-4d NumericPrecisionD=%-4d DatetimePrecision=%-4d tableCLoneManageType=%s\n",ColumnName,DataType,UdtType,OrdinalPosition,CharMaxLength,NumericPrecisionM,NumericPrecisionD,DatetimePrecision,tableCloneManageType);
    }

    public Column clone(){
        Column column = null;
        try {
            column = (Column) super.clone();
            column.setDataSourceType(null);
            column.setTableCatalog(null);
            column.setTableSchema(null);
            column.setTableName(null);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return column;
    }
}
