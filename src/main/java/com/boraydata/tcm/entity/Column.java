package com.boraydata.tcm.entity;

import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageType;


/**
 * @author bufan
 * @data 2021/8/25
 */
public class Column implements Cloneable {
    private DataSourceType dataSourceType;
    // Catalog
    private String tableCatalog;
    // Schema
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String dataType;
    // standard data type
    private String udtType;
    private Integer ordinalPosition;
    // Char data type length
    private Long charMaxLength;
    // numeric data type length
    private Integer numericPrecisionM;
    // numeric data type precision
    private Integer numericPrecisionD;
    // time data type precision
    private Integer datetimePrecision;
    private Boolean isNullAble;
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
        return this.tableCatalog +"."+this.tableSchema +"."+this.tableName +"  col: "+this.columnName +"  type:"+this.dataType;
    }

    public Column setTableCloneManageType(TableCloneManageType tableCloneManageType) {
        this.tableCloneManageType = tableCloneManageType;
        return this;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public Column setTableCatalog(String tableCatalog) {
        this.tableCatalog = tableCatalog;
        return this;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public Column setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public Column setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    public Column setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getDataType() {
        return dataType;
    }

    public Column setDataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    public String getUdtType() {
        return udtType;
    }

    public Column setUdtType(String udtType) {
        this.udtType = udtType;
        return this;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public Column setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
        return this;
    }

    public Boolean getNullAble() {
        return isNullAble;
    }

    public Column setNullAble(Boolean nullAble) {
        isNullAble = nullAble;
        return this;
    }

    public Long getCharMaxLength() {
        return charMaxLength;
    }

    public Column setCharMaxLength(Long charMaxLength) {
        this.charMaxLength = charMaxLength;
        return this;
    }

    public Integer getNumericPrecisionM() {
        return numericPrecisionM;
    }

    public Column setNumericPrecisionM(Integer numericPrecisionM) {
        this.numericPrecisionM = numericPrecisionM;
        return this;
    }

    public Integer getNumericPrecisionD() {
        return numericPrecisionD;
    }

    public Column setNumericPrecisionD(Integer numericPrecisionD) {
        this.numericPrecisionD = numericPrecisionD;
        return this;
    }

    public Integer getDatetimePrecision() {
        return datetimePrecision;
    }

    public Column setDatetimePrecision(Integer datetimePrecision) {
        this.datetimePrecision = datetimePrecision;
        return this;
    }


    @Override
    public String toString() {
        return "Column{" +
                "dataSourceType=" + dataSourceType +
                ", TableCatalog='" + tableCatalog + '\'' +
                ", TableSchema='" + tableSchema + '\'' +
                ", TableName='" + tableName + '\'' +
                ", ColumnName='" + columnName + '\'' +
                ", DataType='" + dataType + '\'' +
                ", UdtType='" + udtType + '\'' +
                ", OrdinalPosition=" + ordinalPosition +
                ", CharMaxLength=" + charMaxLength +
                ", NumericPrecisionM=" + numericPrecisionM +
                ", NumericPrecisionD=" + numericPrecisionD +
                ", DatetimePrecision=" + datetimePrecision +
                ", IsNullAble=" + isNullAble +
                ", tableCloneManageType=" + tableCloneManageType +
                '}';
    }

    public String getInfo(){
        return String.format("ColumnName=%-20s DataType=%-20s UdtType=%-20s Position=%-4d CharMaxLength=%-10d NumericPrecisionM=%-4d NumericPrecisionD=%-4d DatetimePrecision=%-4d tableCLoneManageType=%s\n", columnName, dataType, udtType, ordinalPosition, charMaxLength, numericPrecisionM, numericPrecisionD, datetimePrecision,tableCloneManageType);
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
