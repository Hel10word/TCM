package com.boraydata.cdc.tcm.entity;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;


/**
 * record column metadata information entity
 * @author bufan
 * @date 2021/8/25
 */

@JsonPropertyOrder({
//        "dataSourceEnum",
//        "tableCatalog",
//        "tableSchema",
//        "tableName",
        "columnName",
        "dataType",
//        "udtType",
//        "ordinalPosition",
        "characterMaximumPosition",
        "numericPrecision",
        "numericScale",
        "datetimePrecision",
        "isNullable",
//        "TCMDataTypeEnum",
        "mappingDataType",
})
@JsonIgnoreProperties({"dataSourceEnum","tableCatalog","tableSchema","tableName","udtType","ordinalPosition",})
public class Column implements Cloneable {
    private DataSourceEnum dataSourceEnum;
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
    private Long characterMaximumPosition;
    // numeric data type length
    private Integer numericPrecision;
    // numeric data type precision
    private Integer numericScale;
    // time data type precision
    private Integer datetimePrecision;
    private Boolean nullable;
    // mapping tcm data type
    private TCMDataTypeEnum tcmDataTypeEnum;

    public DataSourceEnum getDataSourceEnum() {
        return dataSourceEnum;
    }

    public Column setDataSourceEnum(DataSourceEnum dataSourceEnum) {
        this.dataSourceEnum = dataSourceEnum;
        return this;
    }

    @JsonGetter("mappingDataType")
    public TCMDataTypeEnum getTcmDataTypeEnum() {
        return tcmDataTypeEnum;
    }
    @JsonSetter("mappingDataType")
    public Column setTcmDataTypeEnum(TCMDataTypeEnum tcmDataTypeEnum) {
        this.tcmDataTypeEnum = tcmDataTypeEnum;
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

    public Boolean getNullable() {
        return nullable;
    }

    public Column setNullable(Boolean nullAble) {
        nullable = nullAble;
        return this;
    }

    public Long getCharacterMaximumPosition() {
        return characterMaximumPosition;
    }

    public Column setCharacterMaximumPosition(Long characterMaximumPosition) {
        this.characterMaximumPosition = characterMaximumPosition;
        return this;
    }

    public Integer getNumericPrecision() {
        return numericPrecision;
    }

    public Column setNumericPrecision(Integer numericPrecision) {
        this.numericPrecision = numericPrecision;
        return this;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public Column setNumericScale(Integer numericScale) {
        this.numericScale = numericScale;
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
        return this.outInfo();
    }

    public String outInfo(){
        return String.format("ColumnName=%-20s DataType=%-20s OrdinalPosition=%-4s CharacterMaximumPosition=%-10s NumericPrecision=%-6s NumericScale=%-6s DatetimePrecision=%-6s Nullable=%-6s TableCLoneManageType=%s \n",
                            columnName,         dataType,      ordinalPosition,    characterMaximumPosition,        numericPrecision,     numericScale,    datetimePrecision, nullable, tcmDataTypeEnum);
    }

    public Column clone(){
        Column column = null;
        try {
            column = (Column) super.clone();
            column.setDataSourceEnum(null);
            column.setTableCatalog(null);
            column.setTableSchema(null);
            column.setTableName(null);
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return column;
    }
}
