package com.boraydata.tcm.core;

import com.boraydata.tcm.entity.Column;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.utils.DatasourceConnectionFactory;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.utils.StringUtil;

import java.sql.*;
import java.util.LinkedList;

/** implement create table structure and clone table structure
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

    public TableCloneManage(DatabaseConfig sourceConfig, DatabaseConfig cloneConfig, MappingTool sourceMappingTool, MappingTool cloneMappingTool) {
        this.sourceConfig = sourceConfig;
        this.cloneConfig = cloneConfig;
        this.sourceMappingTool = sourceMappingTool;
        this.cloneMappingTool = cloneMappingTool;
    }



    //  try to get table struct from Metadata by table name,and find the mapping relationship of each field
    public Table getSourceTable(String tablename){
        Table databaseTable = getDatabaseTable(tablename, sourceConfig);
        return sourceMappingTool.createSourceMappingTable(databaseTable);
    }
    private Table getDatabaseTable(String tablename, DatabaseConfig databaseConfig){
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
        return createTableInCloneDatasource(table,cloneConfig);
    }
    private boolean createTableInCloneDatasource(Table table,DatabaseConfig databaseConfig){
        cloneMappingTool.getCreateTableSQL(table);
        try (Connection conn = DatasourceConnectionFactory.createDataSourceConnection(databaseConfig);
             Statement statement = conn.createStatement();){
            String sql = cloneMappingTool.getCreateTableSQL(table);
//            System.out.println(sql);
            int i = statement.executeUpdate(sql);
            if (i == 0)
                return true;
            else
                throw new TCMException(" create table in "
                        +databaseConfig.getDataSourceType().name()+"."
                        +databaseConfig.getDataSourceType().TableCatalog+"."
                        +databaseConfig.getDataSourceType().TableSchema+"."
                        +table.getTablename()+" is FAILED !!!");
        }catch (Exception e){
            throw new TCMException("Failed to create clone table,maybe datasource connection unable use. in ->"+databaseConfig.getDataSourceType().name());
        }
    }


}
