package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;

/**
 * @author bufan
 * @data 2021/11/4
 */
public class SyncingToolFactory {
    public static SyncingTool create(DataSourceEnum dataSourceEnum){
        if (DataSourceEnum.MYSQL.toString().equals(dataSourceEnum.name()))
            return new MysqlSyncingTool();
        else if (DataSourceEnum.POSTGRESQL.toString().equals(dataSourceEnum.name()))
            return new PgsqlSyncingTool();
        else if (DataSourceEnum.HUDI.toString().equals(dataSourceEnum.name()))
            return new HudiSyncingTool();
        return null;
    }
}
