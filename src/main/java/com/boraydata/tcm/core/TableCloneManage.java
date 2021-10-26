package com.boraydata.tcm.core;

import com.boraydata.tcm.command.SparkCommandGenerate;
import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.utils.FileUtil;
import com.boraydata.tcm.utils.StringUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
 * (1),use SQL ( by *.core.DataSourceType ) query table info and set Table ( name = sourceTable ).
 *
 * (2),use *MappingTool to mapping 'sourceTable' datatype to TCM datatype,
 *     and set Table.columns.DataTypeMapping. ( name = sourceMappingTable )
 *
 * (3),clone 'sourceMappingTable' temp table,set Table.columns.datatype use *.core.DataTypeMapping.( name = cloneTable)
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

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public MappingTool getSourceMappingTool() {
        return sourceMappingTool;
    }

    public MappingTool getCloneMappingTool() {
        return cloneMappingTool;
    }

    public TableCloneManage(){}

    public TableCloneManage(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig, MappingTool sourceMappingTool, MappingTool cloneMappingTool) {
        this.sourceConfig = sourceConfig;
        this.cloneConfig = cloneConfig;
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
    }

    //  try to get table struct from Metadata by table name,and find the mapping relationship of each field
    public Table getSourceTable(String tablename){
        Table databaseTable = getDatabaseTable(sourceConfig,tablename);
        return sourceMappingTool.createSourceMappingTable(databaseTable);
    }
    public Table getDatabaseTable(DatabaseConfig databaseConfig,String tablename){
        DataSourceType dsType = databaseConfig.getDataSourceType();
        try (
                Connection con =DatasourceConnectionFactory.createDataSourceConnection(databaseConfig);
                PreparedStatement ps =  con.prepareStatement(dsType.SQL_TableInfoByTableName);
        ){
            // padding Table_Name
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
                column.setTableCatalog(rs.getString(dsType.TableCatalog));
                column.setTableSchema(rs.getString(dsType.TableSchema));
                column.setTableName(rs.getString(dsType.TableName));
                column.setColumnName(rs.getString(dsType.ColumnName));
                column.setDataType(rs.getString(dsType.DataType));
                column.setOrdinalPosition(rs.getInt(dsType.OrdinalPosition));
                column.setNullAble(rs.getBoolean(dsType.IsNullAble));
                columns.add(column);
            }
            Table table = new Table();
            table.setDataSourceType(databaseConfig.getDataSourceType());
            table.setCatalogname(tablecatalog);
            table.setSchemaname(tableSchema);
            table.setTablename(tablename);
            table.setColumns(columns);
            if(StringUtil.isNullOrEmpty(tablecatalog)&&StringUtil.isNullOrEmpty(tableSchema))
                throw new TCMException("Not Found Table '"+tablename+"' in "
                        +databaseConfig.getDataSourceType().toString()+"."
                +databaseConfig.getDatabasename()+" , you should sure it exist.");
            return table;
        }catch (SQLException e) {
            throw new TCMException("Failed create table('"+tablename+"') information use connection : *.core.TableCloneManage.getDatabaseTable");
        }
    }

    // Clone Table
    public Table mappingCloneTable(Table table){
        return cloneMappingTool.createCloneMappingTable(table);
    }

    // cleart table in clone datasource
    public boolean createTableInCloneDatasource(Table table){
        return createTableInCloneDatasource(table,cloneConfig,false);
    }
    public boolean createTableInCloneDatasource(Table table,boolean outSqlFlag){
        return createTableInCloneDatasource(table,cloneConfig,outSqlFlag);
    }
    private boolean createTableInCloneDatasource(Table table,DatabaseConfig databaseConfig,boolean outSqlFlag){

        if(DataSourceType.SPARK.toString().equals(databaseConfig.getDataSourceType().toString()))
            return createTableInSpark(table,databaseConfig);

        try (Connection conn = DatasourceConnectionFactory.createDataSourceConnection(databaseConfig);
             Statement statement = conn.createStatement()){
            // get table create SQL
            String sql = cloneMappingTool.getCreateTableSQL(table);
            if(outSqlFlag)
                FileUtil.WriteMsgToFile(sql,"./createCloneTableIn"+table.getDataSourceType().toString()+".sql");
            try {
                int i = statement.executeUpdate(sql);
                if (i == 0)
                    return true;
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
        return false;
    }

    private boolean createTableInSpark(Table table,DatabaseConfig databaseConfig){
        SparkCommandGenerate generate = new SparkCommandGenerate();
        String createShell = generate.getConnectCommand(databaseConfig).replace("?",
                cloneMappingTool.getCreateTableSQL(table));

        String createTableShellPath = "/usr/local/createTableInSpark.sh";
        if(!FileUtil.WriteMsgToFile(createShell, createTableShellPath))
            throw new TCMException("create createShell failed");
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command("/bin/sh",createTableShellPath);
            Process start = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(start.getInputStream()))) {
                String line;
                System.out.println("\t  Excuet Shell : "+createTableShellPath);
                while ((line = reader.readLine()) != null)
                    System.out.println("\t Shell Out:"+line);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }

}
