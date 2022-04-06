package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
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
    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private TableCloneManageContext tableCloneManageContext;

    private TableCloneManage() { }

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
        Table originTable = getSourceTableByTableName(this.sourceConfig, tableName);
        Table sourceMappingTable = this.sourceMappingTool.createSourceMappingTable(originTable);
        String createSourceTableSQL = this.sourceMappingTool.getCreateTableSQL(sourceMappingTable);
        this.tableCloneManageContext.setSourceTable(sourceMappingTable);
        this.tableCloneManageContext.setSourceTableSQL(createSourceTableSQL);
        DataMappingSQLTool.checkRelationship(sourceMappingTable,this.sourceMappingTool,this.tableCloneManageContext);

        if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getGetSourceTableSQL())){
            String fileName = sourceMappingTable.getDataSourceType()+"_"+sourceMappingTable.getTableName()+".sql";
            this.tableCloneManageContext.setSourceTableSQLFileName(fileName);
            String sqlFilePath = this.tableCloneManageContext.getTempDirectory()+fileName;
            FileUtil.WriteMsgToFile(createSourceTableSQL,sqlFilePath);

            Table tempTable = this.tableCloneManageContext.getTempTable();
            String sql = this.tableCloneManageContext.getTempTableCreateSQL();
            if(tempTable != null && DataSourceType.MYSQL.equals(tempTable.getSourceType()) && StringUtil.isNullOrEmpty(sql))
                FileUtil.WriteMsgToFile(sql,this.tableCloneManageContext.getTempDirectory()+"TempTable.sql");
        }
        return sourceMappingTable;
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
            String tableCatalog = null;
            String tableSchema = null;
            while (rs.next()){
                Column column = new Column();
                if (StringUtil.isNullOrEmpty(tableCatalog))
                    tableCatalog = rs.getString(dsType.TableCatalog);
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
            if(StringUtil.isNullOrEmpty(tableCatalog)&&StringUtil.isNullOrEmpty(tableSchema))
                throw new TCMException("Not Found Table '"+tableName+"' in "
                        +dbConfig.getDataSourceType().toString()+"."
                        +dbConfig.getDatabasename()+" , you should sure it exist.");
            Table table = new Table();
            table.setDataSourceType(dsType);
            table.setCatalogName(tableCatalog);
            table.setSchemaName(tableSchema);
            table.setTableName(tableName);
            table.setColumns(columns);
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed create table('"+tableName+"') information use connection : *.core.TableCloneManage.getDatabaseTable");
        }
    }


    // generate CloneTable
    // ========================================  CloneTable  ======================================
    public Table createCloneTable(Table table){
        return createCloneTable(table, table.getTableName());
    }
    public Table createCloneTable(Table table,String tableName){
        Table cloneTable;
        if(this.cloneMappingTool == null || DataSourceType.HUDI.equals(cloneConfig.getDataSourceType())) {
            cloneTable = table.clone();
            cloneTable.setTableName(tableName);
        }else{
            cloneTable = this.cloneMappingTool.createCloneMappingTable(table);
            cloneTable.setTableName(tableName);
            String createTableSQL = this.cloneMappingTool.getCreateTableSQL(cloneTable);
            this.tableCloneManageContext.setCloneTableSQL(createTableSQL);
            if(Boolean.TRUE.equals(this.tableCloneManageContext.getTcmConfig().getGetCloneTableSQL())){
                String fileName = cloneTable.getDataSourceType()+"_"+tableName+".sql";
                this.tableCloneManageContext.setCloneTableSQLFileName(fileName);
                String sqlFilePath = this.tableCloneManageContext.getTempDirectory()+fileName;
                FileUtil.WriteMsgToFile(createTableSQL,sqlFilePath);
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
        DataSourceType sourceType = this.sourceConfig.getDataSourceType();
        DataSourceType cloneType = this.cloneConfig.getDataSourceType();
        Table tempTable = tcmContext.getTempTable();
        String tempTableSQL = tcmContext.getTempTableCreateSQL();
        Table cloneTable = tcmContext.getCloneTable();
        String cloneTableSQL = tcmContext.getCloneTableSQL();
        TableCloneManageConfig config = tcmContext.getTcmConfig();

        if(DataSourceType.HUDI.equals(cloneType))
            return true;
        // check the TempTable,at present, only MySQL needs to create temp table
        if(Boolean.TRUE.equals(config.getExecuteExportScript()) && DataSourceType.MYSQL.equals(sourceType) && StringUtil.isNullOrEmpty(tempTableSQL)){
            createTableInDatasource(sourceConfig,tempTableSQL);
        }

        if(Boolean.FALSE.equals(config.getCreateTableInClone()))
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
//            String memsqlColumnStore = this.tableCloneManageContext.getTcmConfig().getMemsqlColumnStore();
//            if(dbConfig.getDataSourceType().equals(DataSourceType.MYSQL) && !StringUtil.isNullOrEmpty(memsqlColumnStore)) {
//                sql = sql.replace(");", ",UNIQUE KEY pk (l_orderkey, l_linenumber) UNENFORCED RELY,SHARD KEY (" + memsqlColumnStore + ") USING CLUSTERED COLUMNSTORE \n );");
////                System.out.println(sql);
//            }
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
                        +dbConfig.getDataSourceType().TableSchema+"\n"+sql+" \nis FAILED !!!");
            }
        }catch (TCMException | SQLException e){
            throw new TCMException("Failed to create clone table,maybe datasource connection unable use!!! -> "+dbConfig.getDataSourceType().toString());
        }
    }



    // ========================================  Export CSV ===========================
    public Boolean exportTableData(){
        if(DataSourceType.HUDI.equals(this.cloneConfig.getDataSourceType()))
            return true;

        String sourceType = this.cloneConfig.getDataSourceType().toString();
        Table table = this.tableCloneManageContext.getFinallySourceTable();
        String exportShellName = "Export_from_"+sourceType+"_"+table.getTableName()+".sh";
        this.tableCloneManageContext.setExportShellName(exportShellName);

        String exportShellPath = this.tableCloneManageContext.getTempDirectory()+exportShellName;

        if(StringUtil.isNullOrEmpty(this.tableCloneManageContext.getCsvFileName()) || this.tableCloneManageContext.getCsvFileName().replaceAll("\"","").replaceAll("'","").length() == 0) {
            String csvFileName = "Export_from_" + sourceType + "_" + table.getTableName() + ".csv";
            this.tableCloneManageContext.setCsvFileName(csvFileName);
        }

        String exportContent = this.sourceSyncingTool.getExportInfo(this.tableCloneManageContext);

        if(!FileUtil.WriteMsgToFile(exportContent,exportShellPath))
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
        String cloneType = this.tableCloneManageContext.getCloneConfig().getDataSourceType().toString();
        Table table = this.tableCloneManageContext.getCloneTable();

        String loadShellName = "Load_to_"+cloneType+"_"+table.getTableName()+".sh";
        this.tableCloneManageContext.setLoadShellName(loadShellName);
        String loadShellPath = this.tableCloneManageContext.getTempDirectory()+loadShellName;
        String loadContent = this.cloneSyncingTool.getLoadInfo(this.tableCloneManageContext);


        if(!FileUtil.WriteMsgToFile(loadContent,loadShellPath))
            throw new TCMException("Failed to Create Export Information In Path:"+loadShellPath+"  Content:"+loadContent);

        if(DataSourceType.HUDI.equals(this.tableCloneManageContext.getCloneConfig().getDataSourceType())){
            String scalaPath = this.tableCloneManageContext.getTempDirectory()+this.tableCloneManageContext.getLoadDataInHudiScalaScriptName();
            String scalaContent = this.tableCloneManageContext.getLoadDataInHudiScalaScriptContent();
            if(!FileUtil.WriteMsgToFile(scalaContent,scalaPath))
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
    // https://cloud.tencent.com/developer/article/1703463
    public void deleteCache(){
        String dir = this.tableCloneManageContext.getTempDirectory();
        String sourcFile = this.tableCloneManageContext.getSourceTableSQLFileName();
        String cloneFile = this.tableCloneManageContext.getCloneTableSQLFileName();
        String csvFile = this.tableCloneManageContext.getCsvFileName();
        String exportShellName = this.tableCloneManageContext.getExportShellName();
        String loadShellName = this.tableCloneManageContext.getLoadShellName();
        String scalaScriptName = this.tableCloneManageContext.getLoadDataInHudiScalaScriptName();
        String hadoopLog = "hadoop.log";

        if(StringUtil.isNullOrEmpty(dir) || !FileUtil.IsDirectory(dir))
            return;

        if(!StringUtil.isNullOrEmpty(sourcFile))
            FileUtil.DeleteFile(dir+sourcFile);
        if(!StringUtil.isNullOrEmpty(cloneFile))
            FileUtil.DeleteFile(dir+cloneFile);
        if(!StringUtil.isNullOrEmpty(exportShellName))
            FileUtil.DeleteFile(dir+exportShellName);
        if(!StringUtil.isNullOrEmpty(loadShellName))
            FileUtil.DeleteFile(dir+loadShellName);
        if(!StringUtil.isNullOrEmpty(csvFile))
            FileUtil.DeleteFile(dir+csvFile);
        if(!StringUtil.isNullOrEmpty(scalaScriptName)) {
            FileUtil.DeleteFile(dir + scalaScriptName);
            FileUtil.DeleteFile(dir + csvFile+"_sample_data");
            FileUtil.DeleteFile(dir+hadoopLog);
        }
    }


}
