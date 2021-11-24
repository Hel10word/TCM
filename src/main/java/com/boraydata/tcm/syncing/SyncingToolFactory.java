package com.boraydata.tcm.syncing;

import com.boraydata.tcm.core.DataSourceType;
/**
 * @author bufan
 * @data 2021/11/4
 */
public class SyncingToolFactory {
    public static SyncingTool create(DataSourceType dataSourceType){
        if (DataSourceType.MYSQL.toString().equals(dataSourceType.name()))
            return new MysqlSyncingTool();
        else if (DataSourceType.POSTGRES.toString().equals(dataSourceType.name()))
            return new PgsqlSyncingTool();
        else if (DataSourceType.HUDI.toString().equals(dataSourceType.name()))
            return new HudiSyncingTool();
        return null;
    }
}
