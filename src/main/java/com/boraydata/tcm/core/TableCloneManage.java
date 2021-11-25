package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.DataMappingSQLTool;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.syncing.SyncingTool;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;

import java.io.File;
import java.sql.*;
import java.util.LinkedList;

/**
 * provide use DatabaseConfig to get Table Information by table name  e.g.: getSourceTable()
 *
 * Mapping the table from the SourceDB to the table of the CloneDB   e.g.: mappingCloneTable()
 *
 *
 * ！！！ init TableCloneManage should provide double DatabaseConfig (sourceConfig、cloneConfig)，
 * ！！！ The following describes the process of initializing sourceConfig table of cloneConfig
 *
 * (1),use SQL query (by *.core.DataSourceType) table info and set Table ( name = sourceTable ).
 *
 * (2),use *MappingTool to mapping 'sourceTable' datatype to TCM datatype,
 *     and set Table.columns.TableCloneManageType. ( name = sourceMappingTable )
 *
 * (3),clone 'sourceMappingTable' temp table,set Table.columns.datatype use *.core.TableCloneManageType.( name = cloneTable)
 *
 * (4),use *MappingTool to create cloneTable Create Table SQL and Execute SQL.
 *
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManage {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private MappingTool sourceMappingTool;
    private MappingTool cloneMappingTool;
    private SyncingTool sourceSyncingTool;
    private SyncingTool cloneSyncingTool;
    private AttachConfig attachConfig;

    private Table sourceMappingTable;
    private Table tempTable;
    private Table cloneTable;

    public TableCloneManage() { }

    public TableCloneManage(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig,
                            MappingTool sourceMappingTool, MappingTool cloneMappingTool,
                            SyncingTool sourceSyncingTool, SyncingTool cloneSyncingTool,
                            AttachConfig attachConfig) {
        this.sourceConfig = sourceConfig;
        this.cloneConfig = cloneConfig;
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
        this.sourceSyncingTool = sourceSyncingTool;
        this.cloneSyncingTool = cloneSyncingTool;
        this.attachConfig = attachConfig;
    }

    public Table getTempTable() {
        return tempTable;
    }

    // find the mapping relationship of each field
    // =======================================  SourceMappingTable  and  check TempTable ==============================
    public Table createSourceMappingTable(String tablename){
        this.sourceMappingTable = sourceMappingTool.createSourceMappingTable(getSourceTableByTablename(sourceConfig, tablename));

        // if source is MySQL,should create TempTable.
        if(this.sourceConfig.getDataSourceType().equals(DataSourceType.MYSQL)){
            for (Column col:sourceMappingTable.getColumns()){
                if(col.getTableCloneManageType().equals(TableCloneManageType.BOOLEAN)||
                        col.getTableCloneManageType().equals(TableCloneManageType.BYTES)){
                    createTempTable(sourceMappingTable);
                    break;
                }
            }
        }else if(this.sourceConfig.getDataSourceType().equals(DataSourceType.POSTGRES)){
            for (Column col:sourceMappingTable.getColumns()){
                if(col.getTableCloneManageType().equals(TableCloneManageType.BOOLEAN)||
                        col.getTableCloneManageType().equals(TableCloneManageType.MONEY)){
                    this.attachConfig.setTempTableSQL(DataMappingSQLTool.getSQL(sourceMappingTable,cloneConfig.getDataSourceType()));
                    break;
                }
            }
        }

        return sourceMappingTable;
    }
    //  try to get table struct from Metadata by table name
    public Table getSourceTableByTablename(DatabaseConfig databaseConfig, String tablename){
        DataSourceType dsType = databaseConfig.getDataSourceType();
        try (
                Connection con =DatasourceConnectionFactory.createDataSourceConnection(databaseConfig);
                PreparedStatement ps =  con.prepareStatement(dsType.SQL_TableInfoByTableName);
        ){
            ps.setString(1,tablename);
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
                throw new TCMException("Not Found Table '"+tablename+"' in "
                        +databaseConfig.getDataSourceType().toString()+"."
                        +databaseConfig.getDatabasename()+" , you should sure it exist.");
            Table table = new Table();
            table.setDataSourceType(dsType);
            table.setCatalogname(tablecatalog);
            table.setSchemaname(tableSchema);
            table.setTablename(tablename);
            table.setColumns(columns);
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed create table('"+tablename+"') information use connection : *.core.TableCloneManage.getDatabaseTable");
        }
    }
    // use SourceMappingTable to Create TempTable
    private Table createTempTable(Table table){
        Table clone = table.clone();
        for (Column col : clone.getColumns())
            col.setDataType(TableCloneManageType.TEXT.getOutDataType(table.getDataSourceType()));
        clone.setDataSourceType(table.getDataSourceType());
        clone.setSchemaname(table.getSchemaname());
        clone.setCatalogname(table.getCatalogname());
        clone.setTablename(table.getTablename()+"_"+StringUtil.getRandom()+"_temp");
        this.tempTable = clone;
        return tempTable;
    }

    // use CloneMappingTool to Create CloneTable
    // ========================================  CloneTable  ======================================
    public Table createCloneTable(Table table){
        return createCloneTable(table,table.getTablename());
    }
    public Table createCloneTable(Table table,String tableName){
        if(table.getDataSourceType().equals(cloneConfig.getDataSourceType()) || DataSourceType.HUDI.equals(cloneConfig.getDataSourceType())) {
            this.cloneTable = table.clone();
            this.cloneTable.setTablename(tableName);
            return cloneTable;
        }

        this.cloneTable = cloneMappingTool.createCloneMappingTable(table);
        this.cloneTable.setTablename(tableName);
        return cloneTable;
    }

    // ========================================  create TempTable && CloneTable in Database ===========================
    public boolean createTableInDatasource(){
        if(DataSourceType.HUDI.equals(cloneConfig.getDataSourceType()))
            return true;

//        if(sourceConfig.getDataSourceType().equals(cloneConfig.getDataSourceType())){
//            return createTableInDatasource(cloneTable,cloneConfig,cloneMappingTool,attachConfig.getDebug(),attachConfig.getTempDirectory());
//        }

        // check the TempTable is create
        if(this.tempTable != null && sourceConfig.getDataSourceType().equals(DataSourceType.MYSQL)){
            if(createTableInDatasource(tempTable,sourceConfig,sourceMappingTool,attachConfig.getDebug(),attachConfig.getTempDirectory())){
                this.attachConfig.setTempTableName(tempTable.getTablename());
                this.attachConfig.setTempTableSQL(DataMappingSQLTool.getSQL(sourceMappingTable,cloneConfig.getDataSourceType()));
            }else
                throw new TCMException("Create TempTable is Failed!!! in "+sourceConfig.getDataSourceType()+"."+tempTable.getTablename());
        }
        if(this.cloneTable != null)
            return createTableInDatasource(cloneTable,cloneConfig,cloneMappingTool,attachConfig.getDebug(),attachConfig.getTempDirectory());
        else
            throw new TCMException("Unable find CloneTable in TCM.");
    }

    // ========================================  Execute SQL By JDBC===========================
    private boolean createTableInDatasource(Table table,DatabaseConfig databaseConfig,MappingTool mappingTool,boolean outSQLFlag,String outSQLDir){
//        if(DataSourceType.SPARK.toString().equals(databaseConfig.getDataSourceType().toString()))
//            return createTableInSpark(table,databaseConfig);
        try (Connection conn = DatasourceConnectionFactory.createDataSourceConnection(databaseConfig);
             Statement statement = conn.createStatement()){
            String sql = mappingTool.getCreateTableSQL(table);
            String outPath = new File(outSQLDir,"createTableIn_" + table.getDataSourceType().toString() + "_" + table.getTablename() + ".sql").getPath();
            if(Boolean.TRUE.equals(outSQLFlag))
                System.out.println(sql);
//                FileUtil.WriteMsgToFile(sql,outPath);
            try {
                int i = statement.executeUpdate(sql);
                if (i == 0)
                    return true;
                else
                    return false;
            }catch (TCMException e){
                throw new TCMException("pls check SQL!! create table in "
                        +databaseConfig.getDataSourceType().name()+"."
                        +databaseConfig.getDataSourceType().TableCatalog+"."
                        +databaseConfig.getDataSourceType().TableSchema+"."
                        +table.getTablename()+" is FAILED !!!");
            }
        }catch (Exception e){
            throw new TCMException("Failed to create clone table,maybe datasource connection unable use!!! -> "+databaseConfig.getDataSourceType().toString());
        }
    }


    // ========================================  Export CSV ===========================
    public boolean exportTableData(){
        String tempDirectory = attachConfig.getTempDirectory();
        String sourceType = sourceConfig.getDataSourceType().toString();
        String cloneType = cloneConfig.getDataSourceType().toString();
        String sourceTable = attachConfig.getRealSourceTableName();
        String csvFileName = sourceType+"_to_"+cloneType+"_"+attachConfig.getCloneTableName()+".csv";

        attachConfig.setCsvFileName(csvFileName);
        attachConfig.setLocalCsvPath(new File(tempDirectory,csvFileName).getPath());
        attachConfig.setExportShellPath(new File(tempDirectory,"Export_from_"+sourceType+"_"+sourceTable+".sh").getPath());
        return sourceSyncingTool.exportFile(sourceConfig,attachConfig);
    }



    // ========================================  Load CSV ===========================

    public boolean loadTableData(){
        String tempDirectory = attachConfig.getTempDirectory();
        String cloneType = cloneConfig.getDataSourceType().toString();
        String cloneTable = attachConfig.getCloneTableName();
        attachConfig.setLoadShellPath(new File(tempDirectory,"Load_to_"+cloneType+"_"+cloneTable+".sh").getPath());
        return cloneSyncingTool.loadFile(cloneConfig,attachConfig);
    }


}
