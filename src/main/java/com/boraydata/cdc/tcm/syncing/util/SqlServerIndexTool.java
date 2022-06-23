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

import static jdk.nashorn.internal.runtime.regexp.joni.encoding.CharacterType.S;

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
 */
public class SqlServerIndexTool {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerIndexTool.class);

    private static final String PRIMARY_KEY_NAME = DataSourceEnum.SQLSERVER.PrimaryKeyName;
    private static final String PRIMARY_KEY_DISABLE = "IS_DISABLED";

    public static Boolean fillingPrimaryKeyName(TableCloneManagerContext tcmContext){
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

        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement getPrimaryNameSta =  con.prepareStatement(genGetPrimaryKey(table));
        ){
            ResultSet rs = getPrimaryNameSta.executeQuery();
            while (rs.next())
                table.setPrimaryKeyName(rs.getString(PRIMARY_KEY_NAME));
        } catch (SQLException e) {
            throw new TCMException("failed to filling primary key name,dbConfig:"+dbConfig.outInfo()+",tableInfo:"+table.outInfo(),e);
        }
        tcmContext.setCloneTable(table);
        return StringUtil.nonEmpty(table.getPrimaryKeyName());
    }

    public static Boolean disableIndex(TableCloneManagerContext tcmContext){
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        Table table = tcmContext.getCloneTable();
        Boolean flag = Boolean.FALSE;
        if(StringUtil.isNullOrEmpty(table.getPrimaryKeyName()))
            fillingPrimaryKeyName(tcmContext);
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement disableIndexSta =  con.prepareStatement(genDisableIndex(table));
                PreparedStatement primaryDisableSta =  con.prepareStatement(genPrimaryDisable(table));
        ){
            disableIndexSta.execute();
            ResultSet rs = primaryDisableSta.executeQuery();
            while (rs.next())
                flag = "1".equals(rs.getString(PRIMARY_KEY_DISABLE))?Boolean.TRUE:Boolean.FALSE;

        } catch (SQLException e) {
            throw new TCMException("failed to disable index,tableInfo:"+table.outInfo(),e);
        }finally {
            if(Boolean.FALSE.equals(flag))
                logger.warn("failed to disable index,tableInfo:{}",table.outInfo());
            return flag;
        }
    }
    public static Boolean rebuildIndex(TableCloneManagerContext tcmContext){
        DatabaseConfig dbConfig = tcmContext.getCloneConfig();
        Table table = tcmContext.getCloneTable();
        Boolean flag = Boolean.TRUE;
        if(StringUtil.isNullOrEmpty(table.getPrimaryKeyName()))
            fillingPrimaryKeyName(tcmContext);
        try (
                Connection con = DatasourceConnectionFactory.createDataSourceConnection(dbConfig);
                PreparedStatement rebuildIndexSta =  con.prepareStatement(genRebuildIndex(table));
                PreparedStatement primaryDisableSta =  con.prepareStatement(genPrimaryDisable(table));
        ){
            rebuildIndexSta.execute();
            ResultSet rs = primaryDisableSta.executeQuery();
            while (rs.next())
                flag = "1".equals(rs.getString(PRIMARY_KEY_DISABLE))?Boolean.TRUE:Boolean.FALSE;
        } catch (SQLException e) {
            throw new TCMException("failed to rebuild index,tableInfo:"+table.outInfo(),e);
        }finally {
            if(Boolean.FALSE.equals(!flag))
                logger.warn("failed to rebuild index,tableInfo:{}",table.outInfo());
            return !flag;
        }
    }


    public static String genGetPrimaryKey(Table table){
        StringBuilder stringBuilder = new StringBuilder("SELECT TOP 1 ")
                .append("CONSTRAINT_NAME as ").append(PRIMARY_KEY_NAME).append(" ")
                .append("FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '")
                .append(table.getTableName());
        if(StringUtil.nonEmpty(table.getSchemaName()))
            stringBuilder.append("' AND TABLE_SCHEMA = '").append(table.getSchemaName());
        return stringBuilder.append("'").toString();
    }
    public static String genDisableIndex(Table table){
        return "ALTER INDEX " +
                table.getPrimaryKeyName() +
                " ON " + table.getTableName() +
                " DISABLE";
    }
    public static String genRebuildIndex(Table table){
        return "ALTER INDEX " +
                table.getPrimaryKeyName() +
                " ON " + table.getTableName() +
                " REBUILD";
    }
    public static String genPrimaryDisable(Table table){
        return "SELECT IS_DISABLED as "+PRIMARY_KEY_DISABLE+
                " FROM SYS.INDEXES WHERE NAME = '"+
                table.getPrimaryKeyName()+"'";
    }
}
