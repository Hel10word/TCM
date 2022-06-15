package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.entity.Table;

/**
 * Record the context information of {@link TableCloneManager}
 * @author bufan
 * @date 2021/8/25
 */
public class TableCloneManagerContext {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private TableCloneManagerConfig tcmConfig;

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

    private TableCloneManagerContext(Builder builder){
        this.sourceConfig = builder.sourceConfig;
        this.cloneConfig = builder.cloneConfig;
        this.tcmConfig = builder.tcmConfig;
        this.tempDirectory = builder.tcmConfig.getTempDirectory();
        this.csvFileName = builder.tcmConfig.getCsvFileName();
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public TableCloneManagerConfig getTcmConfig() {
        return tcmConfig;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public TableCloneManagerContext setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
        return this;
    }

    public Table getTempTable() {
        return tempTable;
    }

    public TableCloneManagerContext setTempTable(Table tempTable) {
        this.tempTable = tempTable;
        return this;
    }

    public Table getCloneTable() {
        return cloneTable;
    }

    public TableCloneManagerContext setCloneTable(Table cloneTable) {
        this.cloneTable = cloneTable;
        return this;
    }

    public String getTempTableCreateSQL() {
        return tempTableCreateSQL;
    }

    public TableCloneManagerContext setTempTableCreateSQL(String tempTableCreateSQL) {
        this.tempTableCreateSQL = tempTableCreateSQL;
        return this;
    }

    public String getTempTableSelectSQL() {
        return tempTableSelectSQL;
    }

    public TableCloneManagerContext setTempTableSelectSQL(String tempTableSelectSQL) {
        this.tempTableSelectSQL = tempTableSelectSQL;
        return this;
    }

    public String getSourceTableSQLFileName() {
        return sourceTableSQLFileName;
    }

    public TableCloneManagerContext setSourceTableSQLFileName(String sourceTableSQLFileName) {
        this.sourceTableSQLFileName = sourceTableSQLFileName;
        return this;
    }

    public String getCloneTableSQLFileName() {
        return cloneTableSQLFileName;
    }

    public TableCloneManagerContext setCloneTableSQLFileName(String cloneTableSQLFileName) {
        this.cloneTableSQLFileName = cloneTableSQLFileName;
        return this;
    }

    public String getCsvFileName() {
        return csvFileName;
    }

    public TableCloneManagerContext setCsvFileName(String csvFileName) {
        this.csvFileName = csvFileName;
        return this;
    }

    public String getExportShellName() {
        return exportShellName;
    }

    public TableCloneManagerContext setExportShellName(String exportShellName) {
        this.exportShellName = exportShellName;
        return this;
    }

    public String getLoadShellName() {
        return loadShellName;
    }

    public TableCloneManagerContext setLoadShellName(String loadShellName) {
        this.loadShellName = loadShellName;
        return this;
    }

    public String getLoadDataInHudiScalaScriptName() {
        return loadDataInHudiScalaScriptName;
    }

    public TableCloneManagerContext setLoadDataInHudiScalaScriptName(String loadDataInHudiScalaScriptName) {
        this.loadDataInHudiScalaScriptName = loadDataInHudiScalaScriptName;
        return this;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public String getSourceTableSQL() {
        return sourceTableSQL;
    }

    public TableCloneManagerContext setSourceTableSQL(String sourceTableSQL) {
        this.sourceTableSQL = sourceTableSQL;
        return this;
    }

    public String getCloneTableSQL() {
        return cloneTableSQL;
    }

    public TableCloneManagerContext setCloneTableSQL(String cloneTableSQL) {
        this.cloneTableSQL = cloneTableSQL;
        return this;
    }

    public String getExportShellContent() {
        return exportShellContent;
    }

    public TableCloneManagerContext setExportShellContent(String exportShellContent) {
        this.exportShellContent = exportShellContent;
        return this;
    }

    public String getLoadShellContent() {
        return loadShellContent;
    }

    public TableCloneManagerContext setLoadShellContent(String loadShellContent) {
        this.loadShellContent = loadShellContent;
        return this;
    }

    public String getLoadDataInHudiScalaScriptContent() {
        return loadDataInHudiScalaScriptContent;
    }

    public TableCloneManagerContext setLoadDataInHudiScalaScriptContent(String loadDataInHudiScalaScriptContent) {
        this.loadDataInHudiScalaScriptContent = loadDataInHudiScalaScriptContent;
        return this;
    }

    public static class Builder{
        private DatabaseConfig sourceConfig;
        private DatabaseConfig cloneConfig;
        private TableCloneManagerConfig tcmConfig;

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

        public Builder setTcmConfig(TableCloneManagerConfig tcmConfig) {
            this.tcmConfig = tcmConfig;
            this.setSourceConfig(tcmConfig.getSourceConfig());
            this.setCloneConfig(tcmConfig.getCloneConfig());
            return this;
        }

        public TableCloneManagerContext create() {
            return new TableCloneManagerContext(this);
        }
    }

}
