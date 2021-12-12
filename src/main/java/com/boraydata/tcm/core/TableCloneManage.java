package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.syncing.CommandExecutor;
import com.boraydata.tcm.syncing.SyncingTool;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;

/**
 * TableCloneManage is used to organize the following functions:
 *  (1).use Source DatabaseConfig 、TableName get Table Metadata info and save in Table Object.
 *              e.g.    Table originTable = this.getSourceTableByTableName(sourceConfig,tableName);
 *
 *  (2).originTable set Mapping Data Type with *.mapping._MappingTool;
 *      ps: This step will determine the relationship between sourceBDType and cloneBDType and Table.Columns.
 *          In order to ensure the normal operation of subsequent Table Data Export and Load,
 *          Temp Table may be generate.
 *              e.g.    Table sourceTable = sourceMappingTool.createSourceMappingTable(originTable).
 *
 *  (3).generate Clone Table Object from previous step,set Table Data Type with *.core.TableCloneManageType.
 *              e.g.    Table cloneTable = cloneMappingTool.createCloneMappingTable(sourceTable).
 *
 *  (4).generate Export Source Table Data shell command. *.syncing._SyncingTool.
 *              e.g.    String exportCommandContent = sourceSyncingTool.exportFile(tableCloneManageContext);
 *
 *  (5).generate Load Source Table Data shell command. *.syncing._SyncingTool.
 *              e.g.    String loadCommandContent = cloneSyncingTool.loadFile(tableCloneManageContext);
 *
 *   Then,you can write this String to a *.sh file by *.utils.WriteMsgToFile(content,filePath).
 *   finally use *.syncing.CommandExecutor(filePath,debugFlag),to execute shell script in your machine.
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
    private TableCloneManageContext tableCloneManageContext;

    public TableCloneManage() { }

    public TableCloneManage(
                            MappingTool sourceMappingTool, MappingTool cloneMappingTool,
                            SyncingTool sourceSyncingTool, SyncingTool cloneSyncingTool,
                            TableCloneManageContext tableCloneManageContext) {
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
        this.sourceSyncingTool = sourceSyncingTool;
        this.cloneSyncingTool = cloneSyncingTool;
        this.tableCloneManageContext = tableCloneManageContext;
    }

    // find the mapping relationship of each field
    // =======================================  Generate Source Mapping Table Object and Temple Table Object  ==============================
    public Table createSourceMappingTable(String tableName){
        Table sourceMappingTable = createSourceMappingTable(tableName, this.sourceMappingTool, this.tableCloneManageContext.getSourceConfig());
        checkRelationship(sourceMappingTable,this.tableCloneManageContext);
        this.tableCloneManageContext.setSourceTable(sourceMappingTable);
        return sourceMappingTable;
    }
    private Table createSourceMappingTable(String tableName,MappingTool mappingTool,DatabaseConfig dbConfig){
        return mappingTool.createSourceMappingTable(getSourceTableByTableName(dbConfig, tableName));
    }

    //  try to get origin table struct information from Metadata by table name
    public Table getSourceTableByTableName(DatabaseConfig dbConfig, String tableName){
        DataSourceType dsType = dbConfig.getDataSourceType();
        try (
                Connection con =DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement ps =  con.prepareStatement(dsType.SQL_TableInfoByTableName);
        ){
            ps.setString(1,tableName);
            ResultSet rs = ps.executeQuery();
            LinkedList<Column> columns = new LinkedList<>();
            String tablecatalog = null;
            String tableSchema = null;
            while (rs.next()){
                Column column = new Column();
                if (StringUtil.isNullOrEmpty(tablecatalog))
                    tablecatalog = rs.getString(dsType.TableCatalog);
                if (StringUtil.isNullOrEmpty(tableSchema))
                    tableSchema = rs.getString(dsType.TableSchema);
                column.setDataSourceType(dsType);
                column.setTableCatalog(rs.getString(dsType.TableCatalog));
                column.setTableSchema(rs.getString(dsType.TableSchema));
                column.setTableName(rs.getString(dsType.TableName));
                column.setColumnName(rs.getString(dsType.ColumnName));
                column.setDataType(rs.getString(dsType.DataType));
                column.setUdtType(rs.getString(dsType.UdtType));
                column.setOrdinalPosition(rs.getInt(dsType.OrdinalPosition));
                column.setNullAble(rs.getBoolean(dsType.IsNullAble));
                column.setCharMaxLength(rs.getLong(dsType.CharMaxLength));
                column.setNumericPrecisionM(rs.getInt(dsType.NumericPrecisionM));
                column.setNumericPrecisionD(rs.getInt(dsType.NumericPrecisionD));
                column.setDatetimePrecision(rs.getInt(dsType.DatetimePrecision));
                columns.add(column);
            }
            if(StringUtil.isNullOrEmpty(tablecatalog)&&StringUtil.isNullOrEmpty(tableSchema))
                throw new TCMException("Not Found Table '"+tableName+"' in "
                        +dbConfig.getDataSourceType().toString()+"."
                        +dbConfig.getDatabasename()+" , you should sure it exist.");
            Table table = new Table();
            table.setDataSourceType(dsType);
            table.setCatalogname(tablecatalog);
            table.setSchemaname(tableSchema);
            table.setTablename(tableName);
            table.setColumns(columns);
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed create table('"+tableName+"') information use connection : *.core.TableCloneManage.getDatabaseTable");
        }
    }

    /**
     *      |   Source  |   Clone   | Boolean | Byte | Money |
     *      | --------- | --------- | ------- | ---- | ----- |
     *      |   MySQL   |    MySQL  |         |  √√√ |       |
     *      |           |    PgSQL  |         |  √√√ |       |
     *      |           |    Hudi   |   √√√   |  √√√ |       |
     *      | --------- | --------- | ------- | ---- | ----- |
     *      |   PgSQL   |    MySQL  |   √√√   |      |  √√√  |
     *      |           |    PgSQL  |         |      |       |
     *      |           |    Hudi   |   √√√   |      |  √√√  |
     *
     *      MySQL Temp need create TempTable，PgSQL just change select statement;
     *
     */
    // According to the relationship and the sourceTable to check whether a tempTable is needed.
    private  void checkRelationship(Table table,TableCloneManageContext tcmContext){
        DataSourceType sourceType = tcmContext.getSourceConfig().getDataSourceType();
        DataSourceType cloneType = tcmContext.getSourceConfig().getDataSourceType();
        for (Column col: table.getColumns()){
            TableCloneManageType colType = col.getTableCloneManageType();
            if(sourceType.equals(DataSourceType.MYSQL)){
                if(colType.equals(TableCloneManageType.BOOLEAN)){
                    if(cloneType.equals(DataSourceType.HUDI)){
                        tcmContext.setTempTable(createTempTable(table));
                        break;
                    }
                }else if (colType.equals(TableCloneManageType.BYTES)){
                    if(cloneType.equals(DataSourceType.MYSQL) || cloneType.equals(DataSourceType.POSTGRES) || cloneType.equals(DataSourceType.HUDI)){
                        tcmContext.setTempTable(createTempTable(table));
                        break;
                    }
                }
            }else if (sourceType.equals(DataSourceType.POSTGRES)){
                if(colType.equals(TableCloneManageType.BOOLEAN) || colType.equals(TableCloneManageType.MONEY)){
                    if(cloneType.equals(DataSourceType.MYSQL) || cloneType.equals(DataSourceType.HUDI)){
                        tcmContext.setTempTable(table.clone());
                        break;
                    }
                }
            }
        }
    }

    // generate TempTable by source Table.clone() , all colDataType become TEXT , Table Name is Random
    private Table createTempTable(Table table){
        Table clone = table.clone();
        for (Column col : clone.getColumns())
            col.setDataType(TableCloneManageType.TEXT.getOutDataType(table.getDataSourceType()));
        clone.setDataSourceType(table.getDataSourceType());
        clone.setTablename(table.getTablename()+"_"+StringUtil.getRandom()+"_temp");
        return clone;
    }

    // generate CloneTable
    // ========================================  CloneTable  ======================================
    public Table createCloneTable(Table table){
        return createCloneTable(table,table.getTablename());
    }
    public Table createCloneTable(Table table,String tableName){
        Table cloneTable = createCloneTable(table, tableName, this.cloneMappingTool, this.tableCloneManageContext);
        this.tableCloneManageContext.setCloneTable(cloneTable);
        return cloneTable;
    }
    private Table createCloneTable(Table table,String tableName,MappingTool mappingTool,TableCloneManageContext tcmContext){
        DataSourceType cloneType = tcmContext.getCloneConfig().getDataSourceType();
        Table cloneTable;
        if(table.getDataSourceType().equals(cloneType) || mappingTool == null) {
            cloneTable = table.clone();
        }else
            cloneTable = mappingTool.createCloneMappingTable(table);
        cloneTable.setTablename(tableName);

        tcmContext.setCloneTable(cloneTable);
        return cloneTable;
    }


    // ========================================  Create TempTable && CloneTable in Databases ===========================
    public boolean createTableInDatasource(){
        return createTableInDatasource(this.sourceMappingTool,this.cloneMappingTool,this.tableCloneManageContext);
    }
    private boolean createTableInDatasource(MappingTool sourceMappingTool,MappingTool cloneMappingTool,TableCloneManageContext tcmContext){
        DatabaseConfig sourceConfig = tcmContext.getSourceConfig();
        DataSourceType sourceType = sourceConfig.getDataSourceType();
        DatabaseConfig cloneConfig = tcmContext.getCloneConfig();
        DataSourceType cloneType = cloneConfig.getDataSourceType();

        Table tempTable = tcmContext.getTempTable();
        Table cloneTable = tcmContext.getCloneTable();
        TableCloneManageConfig tcmConfig = tcmContext.getTcmConfig();
        Boolean outSQLFlag = tcmConfig.getDebug();
        String sqlScriptPath = tcmConfig.getTempDirectory()+"createTableIn_"+cloneConfig.getDataSourceType().toString()+".sql";

        if(DataSourceType.HUDI.equals(cloneType))
            return true;
//        else
//            throw new TCMException("should create cloneMappingTool,check "+cloneType.toString());


        // check the TempTable,at present, only MySQL needs to create temp table
        if(tempTable != null && DataSourceType.MYSQL.equals(sourceType)){
            if(!createTableInDatasource(tempTable,sourceConfig,sourceMappingTool, outSQLFlag, sqlScriptPath))
                throw new TCMException("Create TempTable is Failed!!! in "+sourceType.toString()+"."+tempTable.getTablename());
        }
        if(cloneTable != null)
            return createTableInDatasource(cloneTable,cloneConfig,cloneMappingTool, outSQLFlag, sqlScriptPath);
        else
            throw new TCMException("Unable find CloneTable in TCM.");
    }

    // ========================================  Execute SQL By JDBC===========================
    private boolean createTableInDatasource(Table table,DatabaseConfig dbConfig,MappingTool mappingTool,boolean outSQLFlag,String sqlScriptPath){

        try (Connection conn = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
             Statement statement = conn.createStatement()){
            String sql = mappingTool.getCreateTableSQL(table);
            if(Boolean.TRUE.equals(outSQLFlag))
                FileUtil.WriteMsgToFile(sql,sqlScriptPath);
            try {
                int i = statement.executeUpdate(sql);
                if (i == 0)
                    return true;
                else
                    return false;
            }catch (TCMException e){
                throw new TCMException("pls check SQL!! create table in "
                        +dbConfig.getDataSourceType().name()+"."
                        +dbConfig.getDataSourceType().TableCatalog+"."
                        +dbConfig.getDataSourceType().TableSchema+"."
                        +table.getTablename()+" is FAILED !!!");
            }
        }catch (Exception e){
            throw new TCMException("Failed to create clone table,maybe datasource connection unable use!!! -> "+dbConfig.getDataSourceType().toString());
        }
    }



    // ========================================  Export CSV ===========================
    public boolean exportTableData(){
        if(DataSourceType.HUDI.equals(this.tableCloneManageContext.getSourceConfig().getDataSourceType()))
            return true;

        String sourceType = this.tableCloneManageContext.getSourceConfig().getDataSourceType().toString();
        Table table = this.tableCloneManageContext.getFinallySourceTable();

        String exportShellName = "Export_from_"+sourceType+"_"+table.getTablename()+".sh";
        this.tableCloneManageContext.setExportShellName(exportShellName);
        String exportShellPath = this.tableCloneManageContext.getTempDirectory()+exportShellName;

        String csvFileName = "Export_from_"+sourceType+"_"+table.getTablename()+".csv";
        this.tableCloneManageContext.setCsvFileName(csvFileName);

        String exportCommand = this.sourceSyncingTool.exportFile(this.tableCloneManageContext);

        this.tableCloneManageContext.setExportShellContent(exportCommand);

        if(FileUtil.WriteMsgToFile(exportCommand,exportShellPath)){
           return CommandExecutor.executeShell(exportShellPath,this.tableCloneManageContext.getTcmConfig().getDebug());
        }
        return false;
    }

    // ========================================  Load CSV ===========================

    public boolean loadTableData(){
        String cloneType = this.tableCloneManageContext.getCloneConfig().getDataSourceType().toString();
        Table table = this.tableCloneManageContext.getCloneTable();

        String loadShellName = "Load_to_"+cloneType+"_"+table.getTablename()+".sh";
        this.tableCloneManageContext.setLoadShellName(loadShellName);
        String loadShellPath = this.tableCloneManageContext.getTempDirectory()+loadShellName;
        String loadCommand = this.cloneSyncingTool.loadFile(this.tableCloneManageContext);

        this.tableCloneManageContext.setLoadShellContent(loadCommand);

        if(FileUtil.WriteMsgToFile(loadCommand,loadShellPath)){
            return CommandExecutor.executeShell(loadShellPath,this.tableCloneManageContext.getTcmConfig().getDebug());
        }
        return false;

    }
}
