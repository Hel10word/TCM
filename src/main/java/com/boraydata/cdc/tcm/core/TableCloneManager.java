package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.mapping.DataMappingSQLTool;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.syncing.DataSyncingCSVConfigTool;
import com.boraydata.cdc.tcm.syncing.SyncingTool;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * According to the source table, create  clone table in the sink, and clone the table data.
 * TableCloneManager is used to organize the following functions:
 *  (1).e.g.  {@link #createSourceMappingTable}
 *      First,need to get origin {@link Table} information by {@link #getSourceTableByTableName(DatabaseConfig sourceConfig, String tableName)},
 *      and use {@link MappingTool#createSourceMappingTable(Table)} to find {@link com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum} mapping
 *      relation.
 *          ps: In order to ensure the normal operation of subsequent Table Data Export and Load,Temp Table may be generate.
 *
 *  (2).e.g.  {@link #createCloneTable(Table)}
 *      create clone table based on table information by {@link MappingTool#createCloneMappingTable(Table)}.
 *
 *  (3).e.g.  {@link #createTableInDatasource()}
 *      based on (1) and (2), create clone table on the sink.
 *
 *  (4).e.g. {@link #exportTableData()}
 *      generate relevant export information  {@link SyncingTool#getExportInfo(TableCloneManagerContext)}
 *      execute respective export data methods {@link SyncingTool#executeExport(TableCloneManagerContext)}
 *
 *  (5).e.g. {@link #loadTableData()}
 *      generate relevant load information  {@link SyncingTool#getLoadInfo(TableCloneManagerContext)}
 *      execute respective load data methods {@link SyncingTool#executeLoad(TableCloneManagerContext)}
 *
 *
 * @author bufan
 * @date 2021/8/25
 */
public class TableCloneManager {

    private static final Logger logger = LoggerFactory.getLogger(TableCloneManager.class);

    private MappingTool sourceMappingTool;
    private MappingTool cloneMappingTool;
    private SyncingTool sourceSyncingTool;
    private SyncingTool cloneSyncingTool;
    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private TableCloneManagerContext tableCloneManagerContext;

    private TableCloneManager() {}

    public TableCloneManager(
                            MappingTool sourceMappingTool, MappingTool cloneMappingTool,
                            SyncingTool sourceSyncingTool, SyncingTool cloneSyncingTool,
                            TableCloneManagerContext tableCloneManagerContext) {
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
        this.sourceSyncingTool = sourceSyncingTool;
        this.cloneSyncingTool = cloneSyncingTool;
        this.tableCloneManagerContext = tableCloneManagerContext;
        this.sourceConfig = this.tableCloneManagerContext.getSourceConfig();
        this.cloneConfig = this.tableCloneManagerContext.getCloneConfig();
    }

    // find the mapping relationship of each field
    // =======================================  Generate Source Mapping Table Object and Temple Table Object  ==============================
    public Table createSourceMappingTable(String tableName){
        Table customTable = this.tableCloneManagerContext.getTcmConfig().getCustomTable();
        if(Objects.nonNull(customTable)) {
            this.tableCloneManagerContext.setSourceTable(customTable);
            return customTable;
        }

        DataSourceEnum sourceEnum = this.tableCloneManagerContext.getSourceConfig().getDataSourceEnum();
        DataSourceEnum cloneEnum = this.tableCloneManagerContext.getCloneConfig().getDataSourceEnum();
        Table originTable = getSourceTableByTableName(this.sourceConfig, tableName);
        Table sourceMappingTable = this.sourceMappingTool.createSourceMappingTable(originTable);
        String createSourceTableSQL = this.sourceMappingTool.getCreateTableSQL(sourceMappingTable);
        this.tableCloneManagerContext.setSourceTable(sourceMappingTable);
        this.tableCloneManagerContext.setSourceTableSQL(createSourceTableSQL);
        Table tempTable = DataMappingSQLTool.checkRelationship(sourceMappingTable,sourceEnum,cloneEnum);
        if(Boolean.FALSE.equals(Objects.isNull(tempTable))){
            this.tableCloneManagerContext.setTempTable(tempTable);
            List<String> primaryKeys = tempTable.getPrimaryKeys();
            tempTable.setPrimaryKeys(null);
            this.tableCloneManagerContext.setTempTableCreateSQL(this.sourceMappingTool.getCreateTableSQL(tempTable));
            tempTable.setPrimaryKeys(primaryKeys);
            this.tableCloneManagerContext.setTempTableSelectSQL(DataMappingSQLTool.getMappingDataSQL(sourceMappingTable,cloneEnum));
        }
        if(Boolean.TRUE.equals(this.tableCloneManagerContext.getTcmConfig().getOutSourceTableSQL())){
            String fileName = sourceMappingTable.getDataSourceEnum()+"_"+sourceMappingTable.getTableName()+".sql";
            this.tableCloneManagerContext.setSourceTableSQLFileName(fileName);
            String sqlFilePath = this.tableCloneManagerContext.getTempDirectory()+fileName;
            FileUtil.writeMsgToFile(createSourceTableSQL,sqlFilePath);
            String sql = this.tableCloneManagerContext.getTempTableCreateSQL();
            if(Boolean.FALSE.equals(Objects.isNull(tempTable)) && DataSourceEnum.MYSQL.equals(tempTable.getOriginalDataSourceEnum()) && StringUtil.isNullOrEmpty(sql))
                FileUtil.writeMsgToFile(sql,this.tableCloneManagerContext.getTempDirectory()+"TempTable.sql");
        }
        return sourceMappingTable;
    }

    //  try to get origin table struct information from Metadata by table name
    public Table getSourceTableByTableName(DatabaseConfig dbConfig, String tableName){
        DataSourceEnum dsType = dbConfig.getDataSourceEnum();
        try (
                Connection con =DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement getTableInfoPs =  con.prepareStatement(dsType.SQL_TableInfoByTableName);
                PreparedStatement getPrimaryKeysPs =  con.prepareStatement(dsType.SQL_GetPrimaryKeys);
        ){
            getTableInfoPs.setString(1,tableName);
            ResultSet rs = getTableInfoPs.executeQuery();
            LinkedList<Column> columns = new LinkedList<>();
            String tableCatalog = null;
            String tableSchema = null;
            while (rs.next()){
                // distinguish same table name in PostgreSQL, e.g. test_db.admin.test_table、test_db.public.test_table
                if(!StringUtil.isNullOrEmpty(dbConfig.getCatalog()) && !StringUtil.isNullOrEmpty(dbConfig.getSchema())){
                    if (!(rs.getString(dsType.TableSchema).equals(dbConfig.getSchema()) && rs.getString(dsType.TableCatalog).equals(dbConfig.getCatalog())))
                        continue;
                }
                Column column = new Column();
                if (StringUtil.isNullOrEmpty(tableCatalog))
                    tableCatalog = rs.getString(dsType.TableCatalog);
                if (StringUtil.isNullOrEmpty(tableSchema))
                    tableSchema = rs.getString(dsType.TableSchema);
                column.setDataSourceEnum(dsType);
                column.setTableCatalog(rs.getString(dsType.TableCatalog));
                column.setTableSchema(rs.getString(dsType.TableSchema));
                column.setTableName(rs.getString(dsType.TableName));
                column.setColumnName(rs.getString(dsType.ColumnName));
                column.setDataType(rs.getString(dsType.DataType));
                column.setUdtType(rs.getString(dsType.UdtType));
                column.setOrdinalPosition(rs.getInt(dsType.OrdinalPosition));
                // SQL Server getString = YES but getBoolean = false
                column.setNullable(StringUtil.isBoolean(rs.getString(dsType.IsNullAble)));

                String characterMaximumLength = rs.getString(dsType.CharacterMaximumLength);
                if(StringUtil.isNullOrEmpty(characterMaximumLength))
                    column.setCharacterMaximumPosition(null);
                else
                    column.setCharacterMaximumPosition(Long.valueOf(characterMaximumLength));

                String numericPrecision = rs.getString(dsType.NumericPrecision);
                if(StringUtil.isNullOrEmpty(numericPrecision))
                    column.setNumericPrecision(null);
                else
                    column.setNumericPrecision(Integer.valueOf(numericPrecision));

                String numericScale = rs.getString(dsType.NumericScale);
                if(StringUtil.isNullOrEmpty(numericScale))
                    column.setNumericScale(null);
                else
                    column.setNumericScale(Integer.valueOf(numericScale));

                String datetimePrecision = rs.getString(dsType.DatetimePrecision);
                if(StringUtil.isNullOrEmpty(datetimePrecision))
                    column.setDatetimePrecision(null);
                else
                    column.setDatetimePrecision(Integer.valueOf(datetimePrecision));

                columns.add(column);
            }
            if(StringUtil.isNullOrEmpty(tableCatalog)&&StringUtil.isNullOrEmpty(tableSchema))
                throw new TCMException("Not Found Table '"+tableName+"' in "+dbConfig.outInfo()+" , you should sure it exist,tableCatalog:"+tableCatalog+" tableSchema:"+tableSchema);

            if(DataSourceEnum.POSTGRESQL.equals(dsType)){
                getPrimaryKeysPs.setString(1, dbConfig.getCatalog()+"."+dbConfig.getSchema()+"."+tableName);
            }else {
                getPrimaryKeysPs.setString(1, dbConfig.getCatalog());
                getPrimaryKeysPs.setString(2, dbConfig.getSchema());
                getPrimaryKeysPs.setString(3, tableName);
            }
            rs = getPrimaryKeysPs.executeQuery();
            String primaryKeyName = null;
            LinkedList<String> primaryKeys = new LinkedList<>();
            while (rs.next()) {
                primaryKeys.add(rs.getString(dsType.ColumnName));
                primaryKeyName = rs.getString(dsType.PrimaryKeyName);
            }


            Table table = new Table();
            table.setDataSourceEnum(dsType);
            table.setCatalogName(tableCatalog);
            table.setSchemaName(tableSchema);
            table.setTableName(tableName);
            table.setColumns(columns);
            table.setPrimaryKeyName(primaryKeyName);
            table.setPrimaryKeys(primaryKeys);
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed to get table '"+tableName+"' information use connection : *.core.TableCloneManager.getDatabaseTable",e);
        }
    }


    // generate CloneTable
    // ========================================  CloneTable  ======================================
    public Table createCloneTable(Table table){
        return createCloneTable(table, table.getTableName());
    }
    public Table createCloneTable(Table table,String tableName){
        Table cloneTable;
        if(this.cloneMappingTool == null || DataSourceEnum.HUDI.equals(cloneConfig.getDataSourceEnum())) {
            cloneTable = table.clone();
            cloneTable.setTableName(tableName);
        }else{
            cloneTable = this.cloneMappingTool.createCloneMappingTable(table);
            cloneTable.setTableName(tableName);
            DatabaseConfig cloneConfig = this.tableCloneManagerContext.getCloneConfig();
            if(StringUtil.nonEmpty(cloneConfig.getCatalog()))
                cloneTable.setCatalogName(cloneConfig.getCatalog());
            if(StringUtil.nonEmpty(cloneConfig.getSchema()))
                cloneTable.setSchemaName(cloneConfig.getSchema());
            List<String> primaryKeys = cloneTable.getPrimaryKeys();

            if(Boolean.FALSE.equals(tableCloneManagerContext.getTcmConfig().getCreatePrimaryKeyInClone()))
                cloneTable.setPrimaryKeys(null);
            String createTableSQL = this.cloneMappingTool.getCreateTableSQL(cloneTable);

            cloneTable.setPrimaryKeys(primaryKeys);

            this.tableCloneManagerContext.setCloneTableSQL(createTableSQL);
            if(Boolean.TRUE.equals(this.tableCloneManagerContext.getTcmConfig().getOutCloneTableSQL())){
                String fileName = cloneTable.getDataSourceEnum()+"_"+tableName+".sql";
                this.tableCloneManagerContext.setCloneTableSQLFileName(fileName);
                String sqlFilePath = this.tableCloneManagerContext.getTempDirectory()+fileName;
                FileUtil.writeMsgToFile(createTableSQL,sqlFilePath);
            }
        }
        this.tableCloneManagerContext.setCloneTable(cloneTable);
        return cloneTable;
    }


    // ========================================  Create TempTable && CloneTable in Databases ===========================
    public boolean createTableInDatasource(){
        return createTableInDatasource(this.tableCloneManagerContext);
    }
    private boolean createTableInDatasource(TableCloneManagerContext tcmContext){
        DataSourceEnum sourceType = this.sourceConfig.getDataSourceEnum();
        DataSourceEnum cloneType = this.cloneConfig.getDataSourceEnum();
        String tempTableSQL = tcmContext.getTempTableCreateSQL();
        Table cloneTable = tcmContext.getCloneTable();
        String cloneTableSQL = tcmContext.getCloneTableSQL();
        TableCloneManagerConfig config = tcmContext.getTcmConfig();

        // check the TempTable,at present, only MySQL needs to create temp table
        if(Boolean.TRUE.equals(config.getExecuteExportScript()) && DataSourceEnum.MYSQL.equals(sourceType) && !StringUtil.isNullOrEmpty(tempTableSQL)){
            createTableInDatasource(sourceConfig,tempTableSQL);
        }

        if(Boolean.FALSE.equals(config.getCreateTableInClone()))
            return true;

        if(DataSourceEnum.HUDI.equals(cloneType))
            return true;
        if(cloneTable != null)
            return createTableInDatasource(cloneConfig,cloneTableSQL);
        else
            throw new TCMException("Unable find CloneTable in TCM.");
    }

    // ========================================  Execute SQL By JDBC===========================
    private boolean createTableInDatasource(DatabaseConfig dbConfig,String sql){
        try (Connection conn = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
             Statement statement = conn.createStatement()){
            int i = statement.executeUpdate(sql);
            if (i == 0)
                return true;
            else
                return false;

        }catch (TCMException|SQLException e){
            throw new TCMException("Failed to create clone table,maybe datasource connection unable use!!! \n"+dbConfig.outInfo()+"\n"+sql,e);
        }
    }



    // ========================================  Export CSV ===========================

    public Boolean exportTableData(){
//        if(DataSourceEnum.HUDI.equals(this.cloneConfig.getDataSourceEnum()))
//            return true;
        DataSyncingCSVConfigTool.CompleteContext(this.tableCloneManagerContext);

        String sourceType = this.tableCloneManagerContext.getSourceConfig().getDataSourceEnum().toString();
        Table tempTable = this.tableCloneManagerContext.getTempTable();
        Table sourceTable = this.tableCloneManagerContext.getSourceTable();
        Table table = Objects.isNull(tempTable)?sourceTable:tempTable;
        String exportShellName = "Export_from_"+sourceType+"_"+table.getTableName()+".sh";
        this.tableCloneManagerContext.setExportShellName(exportShellName);

        String exportShellPath = this.tableCloneManagerContext.getTempDirectory()+exportShellName;

        if(StringUtil.isNullOrEmpty(this.tableCloneManagerContext.getCsvFileName()) || this.tableCloneManagerContext.getCsvFileName().replaceAll("\"","").replaceAll("'","").length() == 0) {
            String csvFileName = "Export_from_" + sourceType + "_" + sourceTable.getTableName() + ".csv";
            this.tableCloneManagerContext.setCsvFileName(csvFileName);
        }
        if(null == this.sourceSyncingTool)
            return true;

        String exportContent = this.sourceSyncingTool.getExportInfo(this.tableCloneManagerContext);

        if(!FileUtil.writeMsgToFile(exportContent,exportShellPath))
            throw new TCMException("Failed to Create Export Information In Path:"+exportShellPath+"  Content:"+exportContent);

        if(Boolean.TRUE.equals(this.tableCloneManagerContext.getTcmConfig().getExecuteExportScript())){
            return this.sourceSyncingTool.executeExport(this.tableCloneManagerContext);
        }else {
            logger.warn("Not Execute Export Data Script!!!!!!");
            return true;
        }

    }

    // ========================================  Load CSV ===========================

    public Boolean loadTableData(){
        String cloneType = this.tableCloneManagerContext.getCloneConfig().getDataSourceEnum().toString();
        Table table = this.tableCloneManagerContext.getCloneTable();

        String loadShellName = "Load_to_"+cloneType+"_"+table.getTableName()+".sh";
        this.tableCloneManagerContext.setLoadShellName(loadShellName);
        String loadShellPath = this.tableCloneManagerContext.getTempDirectory()+loadShellName;
        if(null == this.cloneSyncingTool)
            return true;
        String loadContent = this.cloneSyncingTool.getLoadInfo(this.tableCloneManagerContext);


        if(!FileUtil.writeMsgToFile(loadContent,loadShellPath))
            throw new TCMException("Failed to Create Export Information In Path:"+loadShellPath+"  Content:"+loadContent);

        if(DataSourceEnum.HUDI.equals(this.tableCloneManagerContext.getCloneConfig().getDataSourceEnum())){
            String scalaPath = this.tableCloneManagerContext.getTempDirectory()+this.tableCloneManagerContext.getLoadDataInHudiScalaScriptName();
            String scalaContent = this.tableCloneManagerContext.getLoadDataInHudiScalaScriptContent();
            if(!FileUtil.writeMsgToFile(scalaContent,scalaPath))
                throw new TCMException("Failed to Create Export Information In Path:"+scalaPath+"  Content:"+scalaContent);
        }

        if(Boolean.TRUE.equals(this.tableCloneManagerContext.getTcmConfig().getExecuteLoadScript())){
            return this.cloneSyncingTool.executeLoad(this.tableCloneManagerContext);
        }else {
            logger.warn("Not Execute Load Data Script!!!!!!");
            return true;
        }
    }

    public TableCloneManagerContext getTableCloneManagerContext() {
        return tableCloneManagerContext;
    }

    // ========================================  Delete Cache File  ===========================
    public void deleteCache(){
        String dir = this.tableCloneManagerContext.getTempDirectory();
        String sourcFile = this.tableCloneManagerContext.getSourceTableSQLFileName();
        String cloneFile = this.tableCloneManagerContext.getCloneTableSQLFileName();
        String csvFile = this.tableCloneManagerContext.getCsvFileName();
        String exportShellName = this.tableCloneManagerContext.getExportShellName();
        String loadShellName = this.tableCloneManagerContext.getLoadShellName();
        String scalaScriptName = this.tableCloneManagerContext.getLoadDataInHudiScalaScriptName();
        String scalaOut = this.tableCloneManagerContext.getLoadDataInHudiScalaScriptName()+".out";
        String hadoopLog = "hadoop.log";

        if(StringUtil.isNullOrEmpty(dir) || !FileUtil.isDirectory(dir))
            return;

        if(!StringUtil.isNullOrEmpty(sourcFile))
            FileUtil.deleteFile(dir+sourcFile);
        if(!StringUtil.isNullOrEmpty(cloneFile))
            FileUtil.deleteFile(dir+cloneFile);
        if(!StringUtil.isNullOrEmpty(exportShellName))
            FileUtil.deleteFile(dir+exportShellName);
        if(!StringUtil.isNullOrEmpty(loadShellName))
            FileUtil.deleteFile(dir+loadShellName);
        if(!StringUtil.isNullOrEmpty(csvFile))
            FileUtil.deleteFile(dir+csvFile);
        if(!StringUtil.isNullOrEmpty(scalaScriptName)) {
            FileUtil.deleteFile(dir + scalaScriptName);
            FileUtil.deleteFile(dir + scalaOut);
            FileUtil.deleteFile(dir + csvFile+"_sample_data");
            FileUtil.deleteFile(dir+hadoopLog);
        }
    }


}
