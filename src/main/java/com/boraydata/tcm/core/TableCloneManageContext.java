package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.entity.Table;

/** Initiates the connection information of the data sources
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageContext {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private TableCloneManageConfig tcmConfig;

    private Table sourceTable;
    private Table tempTable;
    private Table cloneTable;
    private String tempTableCreateSQL;
    private String tempTableSelectSQL;

    private String sourceTableSQLFileName;
    private String cloneTableSQLFileName;
    private String csvFileName;
    private String exportShellName;
    private String loadShellName;
    private String loadDataInHudiScalaScriptName;
    private String tempDirectory;

    private String sourceTableSQL;
    private String cloneTableSQL;
    private String exportShellContent;
    private String loadShellContent;
    private String loadDataInHudiScalaScriptContent;

    private TableCloneManageContext(Builder builder){
        this.sourceConfig = builder.sourceConfig;
        this.cloneConfig = builder.cloneConfig;
        this.tcmConfig = builder.tcmConfig;
        this.tempDirectory = builder.tcmConfig.getTempDirectory();
        this.csvFileName = builder.tcmConfig.getCsvFileName();
    }

    public Table getFinallySourceTable(){
        if(this.tempTable != null)
            return this.tempTable;
        return this.sourceTable;
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public TableCloneManageConfig getTcmConfig() {
        return tcmConfig;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public TableCloneManageContext setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public Table getTempTable() {
        return tempTable;
    }

    public TableCloneManageContext setTempTable(Table tempTable) {
        this.tempTable = tempTable;
        return this;
    }

    public Table getCloneTable() {
        return cloneTable;
    }

    public TableCloneManageContext setCloneTable(Table cloneTable) {
        this.cloneTable = cloneTable;
        return this;
    }

    public String getTempTableCreateSQL() {
        return tempTableCreateSQL;
    }

    public TableCloneManageContext setTempTableCreateSQL(String tempTableCreateSQL) {
        this.tempTableCreateSQL = tempTableCreateSQL;
        return this;
    }

    public String getTempTableSelectSQL() {
        return tempTableSelectSQL;
    }

    public TableCloneManageContext setTempTableSelectSQL(String tempTableSelectSQL) {
        this.tempTableSelectSQL = tempTableSelectSQL;
        return this;
    }

    public String getSourceTableSQLFileName() {
        return sourceTableSQLFileName;
    }

    public TableCloneManageContext setSourceTableSQLFileName(String sourceTableSQLFileName) {
        this.sourceTableSQLFileName = sourceTableSQLFileName;
        return this;
    }

    public String getCloneTableSQLFileName() {
        return cloneTableSQLFileName;
    }

    public TableCloneManageContext setCloneTableSQLFileName(String cloneTableSQLFileName) {
        this.cloneTableSQLFileName = cloneTableSQLFileName;
        return this;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public TableCloneManageContext setCsvFileName(String csvFileName) {
        this.csvFileName = csvFileName;
        return this;
    }

    public String getExportShellName() {
        return exportShellName;
    }

    public TableCloneManageContext setExportShellName(String exportShellName) {
        this.exportShellName = exportShellName;
        return this;
    }

    public String getLoadShellName() {
        return loadShellName;
    }

    public TableCloneManageContext setLoadShellName(String loadShellName) {
        this.loadShellName = loadShellName;
        return this;
    }

    public String getLoadDataInHudiScalaScriptName() {
        return loadDataInHudiScalaScriptName;
    }

    public TableCloneManageContext setLoadDataInHudiScalaScriptName(String loadDataInHudiScalaScriptName) {
        this.loadDataInHudiScalaScriptName = loadDataInHudiScalaScriptName;
        return this;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public String getSourceTableSQL() {
        return sourceTableSQL;
    }

    public TableCloneManageContext setSourceTableSQL(String sourceTableSQL) {
        this.sourceTableSQL = sourceTableSQL;
        return this;
    }

    public String getCloneTableSQL() {
        return cloneTableSQL;
    }

    public TableCloneManageContext setCloneTableSQL(String cloneTableSQL) {
        this.cloneTableSQL = cloneTableSQL;
        return this;
    }

    public String getExportShellContent() {
        return exportShellContent;
    }

    public TableCloneManageContext setExportShellContent(String exportShellContent) {
        this.exportShellContent = exportShellContent;
        return this;
    }

    public String getLoadShellContent() {
        return loadShellContent;
    }

    public TableCloneManageContext setLoadShellContent(String loadShellContent) {
        this.loadShellContent = loadShellContent;
        return this;
    }

    public String getLoadDataInHudiScalaScriptContent() {
        return loadDataInHudiScalaScriptContent;
    }

    public TableCloneManageContext setLoadDataInHudiScalaScriptContent(String loadDataInHudiScalaScriptContent) {
        this.loadDataInHudiScalaScriptContent = loadDataInHudiScalaScriptContent;
        return this;
    }

    public static class Builder{
        private DatabaseConfig sourceConfig;
        private DatabaseConfig cloneConfig;
        private TableCloneManageConfig tcmConfig;

        public Builder() {
        }

        public Builder setSourceConfig(DatabaseConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public Builder setCloneConfig(DatabaseConfig cloneConfig) {
            this.cloneConfig = cloneConfig;
            return this;
        }

        public Builder setTcmConfig(TableCloneManageConfig tcmConfig) {
            this.tcmConfig = tcmConfig;
            this.setSourceConfig(tcmConfig.getSourceConfig());
            this.setCloneConfig(tcmConfig.getCloneConfig());
            return this;
        }

        public TableCloneManageContext create() {
            return new TableCloneManageContext(this);
        }
    }

}
