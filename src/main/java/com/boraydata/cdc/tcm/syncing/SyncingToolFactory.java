package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;

/**
 * @author bufan
 * @date 2021/11/4
 */
public class SyncingToolFactory {
    public static SyncingTool create(DataSourceEnum dataSourceEnum){

        switch (dataSourceEnum){
            case MYSQL:
                return new MysqlSyncingTool();
            case POSTGRESQL:
                return new PgsqlSyncingTool();
            case SQLSERVER:
                return new SqlServerSyncingTool();
            case HUDI:
                return new HudiSyncingTool();
            default:
                return null;
        }
    }
}
