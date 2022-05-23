package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.common.TableCloneManageConfig;
import com.boraydata.cdc.tcm.mapping.DataMappingSQLTool;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.syncing.SyncingTool;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.Objects;

/**
 * According to the source table, create  clone table in the sink, and clone the table data.
 * TableCloneManage is used to organize the following functions:
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
 *      generate relevant export information  {@link SyncingTool#getExportInfo(TableCloneManageContext)}
 *      execute respective export data methods {@link SyncingTool#executeExport(TableCloneManageContext)}
 *
 *  (5).e.g. {@link #loadTableData()}
 *      generate relevant load information  {@link SyncingTool#getLoadInfo(TableCloneManageContext)}
 *      execute respective load data methods {@link SyncingTool#executeLoad(TableCloneManageContext)}
 *
 *
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManage {

    private static final Logger logger = LoggerFactory.getLogger(TableCloneManage.class);

    private MappingTool sourceMappingTool;
    private MappingTool cloneMappingTool;
    private SyncingTool sourceSyncingTool;
    private SyncingTool cloneSyncingTool;
    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private TableCloneManageContext tableCloneManageContext;

    private TableCloneManage() {}

    public TableCloneManage(
                            MappingTool sourceMappingTool, MappingTool cloneMappingTool,
                            SyncingTool sourceSyncingTool, SyncingTool cloneSyncingTool,
                            TableCloneManageContext tableCloneManageContext) {
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
        this.sourceSyncingTool = sourceSyncingTool;
        this.cloneSyncingTool = cloneSyncingTool;
        this.tableCloneManageContext = tableCloneManageContext;
        this.sourceConfig = this.tableCloneManageContext.getSourceConfig();
        this.cloneConfig = this.tableCloneManageContext.getCloneConfig();
    }

    // find the mapping relationship of each field
    // =======================================  Generate Source Mapping Table Object and Temple Table Object  ==============================
    public Table createSourceMappingTable(String tableName){
        Table customTable = this.tableCloneManageContext.getTcmConfig().getCustomTable();
        if(Objects.nonNull(customTable)) {
            this.tableCloneManageContext.setSourceTable(customTable);
            return customTable;
        }

        DataSourceEnum sourceEnum = this.tableCloneManageContext.getSourceConfig().getDataSourceEnum();
        DataSourceEnum cloneEnum = this.tableCloneManageContext.getCloneConfig().getDataSourceEnum();
        Table originTable = getSourceTableByTableName(this.sourceConfig, tableName);
        Table sourceMappingTable = this.sourceMappingTool.createSourceMappingTable(originTable);
        String createSourceTableSQL = this.sourceMappingTool.getCreateTableSQL(sourceMappingTable);
        this.tableCloneManageContext.setSourceTable(sourceMappingTable);
        this.tableCloneManageContext.setSourceTableSQL(createSourceTableSQL);
        Table tempTable = DataMappingSQLTool.checkRelationship(sourceMappingTable,sourceEnum,cloneEnum);
        if(Boolean.FALSE.equals(Objects.isNull(tempTable))){
            this.tableCloneManageContext.setTempTable(tempTable);
            this.tableCloneManageContext.setTempTableCreateSQL(this.sourceMappingTool.getCreateTableSQL(tempTable));
            this.tableCloneManageContext.setTempTableSelectSQL(DataMappingSQLTool.getMappingDataSQL(sourceMappingTable,cloneEnum));
        }
        if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getOutSourceTableSQL())){
            String fileName = sourceMappingTable.getDataSourceEnum()+"_"+sourceMappingTable.getTableName()+".sql";
            this.tableCloneManageContext.setSourceTableSQLFileName(fileName);
            String sqlFilePath = this.tableCloneManageContext.getTempDirectory()+fileName;
            FileUtil.writeMsgToFile(createSourceTableSQL,sqlFilePath);
            String sql = this.tableCloneManageContext.getTempTableCreateSQL();
            if(Boolean.FALSE.equals(Objects.isNull(tempTable)) && DataSourceEnum.MYSQL.equals(tempTable.getOriginalDataSourceEnum()) && StringUtil.isNullOrEmpty(sql))
                FileUtil.writeMsgToFile(sql,this.tableCloneManageContext.getTempDirectory()+"TempTable.sql");
        }
        return sourceMappingTable;
    }

    //  try to get origin table struct information from Metadata by table name
    public Table getSourceTableByTableName(DatabaseConfig dbConfig, String tableName){
        DataSourceEnum dsType = dbConfig.getDataSourceEnum();
        try (
                Connection con =DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement ps =  con.prepareStatement(dsType.SQL_TableInfoByTableName);
        ){
            ps.setString(1,tableName);
            ResultSet rs = ps.executeQuery();
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
                column.setNullable(rs.getBoolean(dsType.IsNullAble));
                column.setCharacterMaximumPosition(rs.getLong(dsType.CharMaxLength));
                column.setNumericPrecision(rs.getInt(dsType.NumericPrecisionM));
                column.setNumericScale(rs.getInt(dsType.NumericPrecisionD));
                column.setDatetimePrecision(rs.getInt(dsType.DatetimePrecision));
                columns.add(column);
            }
            if(StringUtil.isNullOrEmpty(tableCatalog)&&StringUtil.isNullOrEmpty(tableSchema))
                throw new TCMException("Not Found Table '"+tableName+"' in "
                        +dbConfig.getDataSourceEnum().toString()+"."
                        +dbConfig.getDatabaseName()+" , you should sure it exist.");
            Table table = new Table();
            table.setDataSourceEnum(dsType);
            table.setCatalogName(tableCatalog);
            table.setSchemaName(tableSchema);
            table.setTableName(tableName);
            table.setColumns(columns);
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed create table('"+tableName+"') information use connection : *.core.TableCloneManage.getDatabaseTable",e);
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
            DatabaseConfig cloneConfig = this.tableCloneManageContext.getCloneConfig();
            if(Boolean.FALSE.equals(StringUtil.isNullOrEmpty(cloneConfig.getCatalog())))
                cloneTable.setCatalogName(cloneConfig.getCatalog());
            if(Boolean.FALSE.equals(StringUtil.isNullOrEmpty(cloneConfig.getSchema())))
                cloneTable.setSchemaName(cloneConfig.getSchema());
            String createTableSQL = this.cloneMappingTool.getCreateTableSQL(cloneTable);
            this.tableCloneManageContext.setCloneTableSQL(createTableSQL);
            if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getOutCloneTableSQL())){
                String fileName = cloneTable.getDataSourceEnum()+"_"+tableName+".sql";
                this.tableCloneManageContext.setCloneTableSQLFileName(fileName);
                String sqlFilePath = this.tableCloneManageContext.getTempDirectory()+fileName;
                FileUtil.writeMsgToFile(createTableSQL,sqlFilePath);
            }
        }
        this.tableCloneManageContext.setCloneTable(cloneTable);
        return cloneTable;
    }


    // ========================================  Create TempTable && CloneTable in Databases ===========================
    public boolean createTableInDatasource(){
//        if(Boolean.FALSE.equals(this.tableCloneManageContext.getTcmConfig().getCreateTableInClone()) && Boolean.FALSE.equals(this.tableCloneManageContext.getTcmConfig().getExecuteExportScript())) {
//            logger.warn("not create table in clone databases!!!");
//            return true;
//        }else{
//            return createTableInDatasource(this.tableCloneManageContext);
//        }
        return createTableInDatasource(this.tableCloneManageContext);
    }
    private boolean createTableInDatasource(TableCloneManageContext tcmContext){
        DataSourceEnum sourceType = this.sourceConfig.getDataSourceEnum();
        DataSourceEnum cloneType = this.cloneConfig.getDataSourceEnum();
        String tempTableSQL = tcmContext.getTempTableCreateSQL();
        Table cloneTable = tcmContext.getCloneTable();
        String cloneTableSQL = tcmContext.getCloneTableSQL();
        TableCloneManageConfig config = tcmContext.getTcmConfig();

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

        String sourceType = this.cloneConfig.getDataSourceEnum().toString();
        Table tempTable = this.tableCloneManageContext.getTempTable();
        Table sourceTable = this.tableCloneManageContext.getSourceTable();
        Table table = Objects.isNull(tempTable)?sourceTable:tempTable;
        String exportShellName = "Export_from_"+sourceType+"_"+table.getTableName()+".sh";
        this.tableCloneManageContext.setExportShellName(exportShellName);

        String exportShellPath = this.tableCloneManageContext.getTempDirectory()+exportShellName;

        if(StringUtil.isNullOrEmpty(this.tableCloneManageContext.getCsvFileName()) || this.tableCloneManageContext.getCsvFileName().replaceAll("\"","").replaceAll("'","").length() == 0) {
            String csvFileName = "Export_from_" + sourceType + "_" + table.getTableName() + ".csv";
            this.tableCloneManageContext.setCsvFileName(csvFileName);
        }

        String exportContent = this.sourceSyncingTool.getExportInfo(this.tableCloneManageContext);

        if(!FileUtil.writeMsgToFile(exportContent,exportShellPath))
            throw new TCMException("Failed to Create Export Information In Path:"+exportShellPath+"  Content:"+exportContent);

        if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getExecuteExportScript())){
            return this.sourceSyncingTool.executeExport(this.tableCloneManageContext);
        }else {
            logger.warn("Not Execute Export Data Script!!!!!!");
            return true;
        }

    }

    // ========================================  Load CSV ===========================

    public Boolean loadTableData(){
        String cloneType = this.tableCloneManageContext.getCloneConfig().getDataSourceEnum().toString();
        Table table = this.tableCloneManageContext.getCloneTable();

        String loadShellName = "Load_to_"+cloneType+"_"+table.getTableName()+".sh";
        this.tableCloneManageContext.setLoadShellName(loadShellName);
        String loadShellPath = this.tableCloneManageContext.getTempDirectory()+loadShellName;
        String loadContent = this.cloneSyncingTool.getLoadInfo(this.tableCloneManageContext);


        if(!FileUtil.writeMsgToFile(loadContent,loadShellPath))
            throw new TCMException("Failed to Create Export Information In Path:"+loadShellPath+"  Content:"+loadContent);

        if(DataSourceEnum.HUDI.equals(this.tableCloneManageContext.getCloneConfig().getDataSourceEnum())){
            String scalaPath = this.tableCloneManageContext.getTempDirectory()+this.tableCloneManageContext.getLoadDataInHudiScalaScriptName();
            String scalaContent = this.tableCloneManageContext.getLoadDataInHudiScalaScriptContent();
            if(!FileUtil.writeMsgToFile(scalaContent,scalaPath))
                throw new TCMException("Failed to Create Export Information In Path:"+scalaPath+"  Content:"+scalaContent);
        }

        if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getExecuteLoadScript())){
            return this.cloneSyncingTool.executeLoad(this.tableCloneManageContext);
        }else {
            logger.warn("Not Execute Load Data Script!!!!!!");
            return true;
        }
    }






    // ========================================  Delete Cache File  ===========================
    public void deleteCache(){
        String dir = this.tableCloneManageContext.getTempDirectory();
        String sourcFile = this.tableCloneManageContext.getSourceTableSQLFileName();
        String cloneFile = this.tableCloneManageContext.getCloneTableSQLFileName();
        String csvFile = this.tableCloneManageContext.getCsvFileName();
        String exportShellName = this.tableCloneManageContext.getExportShellName();
        String loadShellName = this.tableCloneManageContext.getLoadShellName();
        String scalaScriptName = this.tableCloneManageContext.getLoadDataInHudiScalaScriptName();
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
            FileUtil.deleteFile(dir + csvFile+"_sample_data");
            FileUtil.deleteFile(dir+hadoopLog);
        }
    }


}
