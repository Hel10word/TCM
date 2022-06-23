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
                return new MySQLSyncingTool();
            case POSTGRESQL:
                return new PostgreSQLSyncingTool();
            case SQLSERVER:
                return new SqlServerSyncingTool();
            case HUDI:
                return new HudiSyncingTool();
            default:
                return null;
        }
    }
}
