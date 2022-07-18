package com.boraydata.cdc.tcm.syncing.util;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


/**
 * @author : bufan
 * @date : 2022/6/22
 * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/indexes/clustered-and-nonclustered-indexes-described?view=sql-server-linux-ver15"></a>
 *
 * -- get Primary Index
 * SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 't1' AND TABLE_SCHEMA = 'dbo'
 * -- disable
 * ALTER INDEX PK__t2__3915638543A70640 ON t2 DISABLE;
 * -- rebuild
 * ALTER INDEX PK__t2__3915638543A70640 ON t2 REBUILD;
 * -- show is_disable
 * SELECT IS_DISABLED FROM SYS.INDEXES WHERE NAME = 'PK__t2__3915638543A70640'
 *
 * -- get all NONCLUSTERED index
 * SELECT name from sys.indexes WHERE object_id = OBJECT_ID(N'dbo.lineitem_sf10', N'U') AND type_desc = 'NONCLUSTERED'
 *
 * -- create NONCLUSTERED index
 * CREATE NONCLUSTERED INDEX NONCLUSTERED_lineitem_sf10_l_orderkey ON dbo.lineitem_sf10 (l_orderkey)
 *
 * -- create UNIQUE index
 * CREATE UNIQUE INDEX UNIQUE_lineitem_sf10_l_orderkey ON dbo.lineitem_sf10 (l_orderkey)
 *
 */
public class SqlServerIndexTool {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerIndexTool.class);

//    private static final String PRIMARY_KEY_NAME = DataSourceEnum.SQLSERVER.PrimaryKeyName;
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String INDEX_DISABLE = "IS_DISABLED";


    public static boolean disableIndex(TableCloneManagerContext tcmContext){
        if(Boolean.FALSE.equals(checkInformation(tcmContext)))
            return false;
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        Table cloneTable = tcmContext.getCloneTable();
        String tableName = cloneTable.getSchemaName()+"."+cloneTable.getTableName();

        List<String> allNonClusteredIndexList = getAllNonClusteredIndexList(dbConfig,tableName);
        Boolean flag = Boolean.TRUE;
        if(allNonClusteredIndexList.isEmpty()){
            logger.info("SQL Server Table have not NonClustered Index");
            return flag;
        }
        for (String indexName : allNonClusteredIndexList)
            flag = flag&&disableIndex(dbConfig,tableName,indexName);
        logger.info("Disable SQL Server NonClustered Index: {} status: {}",String.join(",", allNonClusteredIndexList),flag);
        return flag;
    }
    public static boolean rebuildIndex(TableCloneManagerContext tcmContext){
        if(Boolean.FALSE.equals(checkInformation(tcmContext)))
            return false;
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        Table cloneTable = tcmContext.getCloneTable();
        String tableName = cloneTable.getSchemaName()+"."+cloneTable.getTableName();

        List<String> allNonClusteredIndexList = getAllNonClusteredIndexList(dbConfig,tableName);
        Boolean flag = Boolean.TRUE;
        if(allNonClusteredIndexList.isEmpty()){
            return flag;
        }
        for (String indexName : allNonClusteredIndexList)
            flag = flag&&rebuildIndex(dbConfig,tableName,indexName);
        logger.info("Rebuild SQL Server NonClustered Index: {} status: {}",String.join(",", allNonClusteredIndexList),flag);
        return flag;
    }

    public static boolean checkInformation(TableCloneManagerContext tcmContext){
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        if(!DataSourceEnum.SQLSERVER.equals(dbConfig.getDataSourceEnum())){
            String dbInfo = dbConfig.outInfo();
            logger.warn("only 'SQL Server' datasource type need disable index in load data, clone database config:{}",dbInfo);
            return Boolean.FALSE;
        }
        Table table = tcmContext.getCloneTable();
        if (StringUtil.nonEmpty(table.getCatalogName()) && !table.getCatalogName().equals(dbConfig.getDatabaseName()))
            logger.warn("table catalog name incorrect,DatabaseName:{},tableInfo:{}",dbConfig.getDatabaseName(),table.outInfo());
        if(StringUtil.isNullOrEmpty(table.getTableName()) || !table.getTableName().equals(tcmContext.getTcmConfig().getCloneTableName()))
            logger.warn("table name incorrect,cloneTableName:{},tableInfo:{}",tcmContext.getTcmConfig().getCloneTableName(),table.outInfo());
        if (StringUtil.isNullOrEmpty(table.getSchemaName()))
            table.setSchemaName("dbo");
        tcmContext.setCloneTable(table);
        return Boolean.TRUE;
    }



    public static String getPrimaryKeyName(DatabaseConfig dbConfig,Table table){
        String primaryKeyName = null;
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement getPrimaryNameSta =  con.prepareStatement(genGetPrimaryKey(table.getTableName(),table.getSchemaName()));
        ){
            ResultSet rs = getPrimaryNameSta.executeQuery();
            while (rs.next())
                primaryKeyName = rs.getString(INDEX_NAME);
        } catch (SQLException e) {
            throw new TCMException("failed to filling primary key name,dbConfig:"+dbConfig.outInfo()+",tableInfo:"+table.outInfo(),e);
        }
        return primaryKeyName;
    }

    public static List<String> getAllNonClusteredIndexList (DatabaseConfig dbConfig,String tableName){
        List<String> list = new LinkedList(Collections.emptyList());
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement getPrimaryNameSta =  con.prepareStatement(genGetAllNonClusteredIndex(tableName));
        ){
            ResultSet rs = getPrimaryNameSta.executeQuery();
            while (rs.next())
                list.add(rs.getString(INDEX_NAME));
        } catch (SQLException e) {
            throw new TCMException("failed to filling primary key name,dbConfig:"+dbConfig.outInfo()+",tableName:"+tableName,e);
        }
        return list;
    }

    public static Boolean disableIndex(DatabaseConfig dbConfig,String tableName,String indexName){
        Boolean flag = Boolean.FALSE;
        String isDisable = null;
        if(StringUtil.isNullOrEmpty(tableName) || StringUtil.isNullOrEmpty(indexName))
            return false;
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement disableIndexSta =  con.prepareStatement(genDisableIndex(tableName, indexName));
                PreparedStatement primaryDisableSta =  con.prepareStatement(genIndexDisableSQL(indexName));
        ){
            disableIndexSta.execute();
            ResultSet rs = primaryDisableSta.executeQuery();
            while (rs.next()) {
                isDisable = rs.getString(INDEX_DISABLE);
                flag = "1".equals(isDisable) ? Boolean.TRUE : Boolean.FALSE;
            }

        } catch (SQLException e) {
            throw new TCMException("failed to rebuild index,Table Name:"+tableName+" Index Name:"+indexName,e);
        }finally {
            if(Boolean.FALSE.equals(flag))
                logger.warn("failed to rebuild index,Table Name:{} Index Name:{}",tableName,indexName);
            return flag;
        }
    }
    public static Boolean rebuildIndex(DatabaseConfig dbConfig,String tableName,String indexName){
        Boolean flag = Boolean.TRUE;
        String isrebuild = null;
        if(StringUtil.isNullOrEmpty(tableName) || StringUtil.isNullOrEmpty(indexName))
            return false;
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement rebuildIndexSta =  con.prepareStatement(genRebuildIndexSQL(tableName,indexName));
                PreparedStatement primaryDisableSta =  con.prepareStatement(genIndexDisableSQL(indexName));
        ){
            rebuildIndexSta.execute();
            ResultSet rs = primaryDisableSta.executeQuery();
            while (rs.next()) {
                isrebuild = rs.getString(INDEX_DISABLE);
                flag = "1".equals(isrebuild) ? Boolean.TRUE : Boolean.FALSE;
            }
        } catch (SQLException e) {
            throw new TCMException("failed to rebuild index,Table Name:"+tableName+" Index Name:"+indexName,e);
        }finally {
            if(Boolean.FALSE.equals(!flag))
                logger.warn("failed to rebuild index,Table Name:{} Index Name:{}",tableName,indexName);
            return !flag;
        }
    }



    public static String genGetPrimaryKey(String tableName,String schemaName){
        StringBuilder stringBuilder = new StringBuilder("SELECT TOP 1 ")
                .append("CONSTRAINT_NAME as ").append(INDEX_NAME).append(" ")
                .append("FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '")
                .append(tableName);
        if(StringUtil.isNullOrEmpty(schemaName))
            schemaName = "dbo";
        stringBuilder.append("' AND TABLE_SCHEMA = '").append(schemaName).append("'");
        return stringBuilder.toString();
    }
    public static String genGetAllNonClusteredIndex(String tableName){
        StringBuilder stringBuilder = new StringBuilder("SELECT name as ").append(INDEX_NAME).append(" FROM SYS.INDEXES ");
        stringBuilder.append("WHERE object_id = OBJECT_ID(N'")
                .append(tableName)
                .append("', N'U') AND type_desc = 'NONCLUSTERED' ORDER BY index_id ASC");
        return stringBuilder.toString();
    }
    public static String genDisableIndex(String tableName,String indexName){
        return "ALTER INDEX " +
                indexName +
                " ON " + tableName +
                " DISABLE";
    }
    public static String genRebuildIndexSQL(String tableName,String indexName){
        return "ALTER INDEX " +
                indexName +
                " ON " + tableName +
                " REBUILD";
    }
    public static String genIndexDisableSQL(String indexName){
        return "SELECT IS_DISABLED as "+ INDEX_DISABLE +
                " FROM SYS.INDEXES WHERE NAME = '"+
                indexName+"'";
    }
}
