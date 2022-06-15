package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.syncing.HudiSyncingTool;
import com.boraydata.cdc.tcm.syncing.MysqlSyncingTool;
import com.boraydata.cdc.tcm.syncing.PgsqlSyncingTool;
import com.boraydata.cdc.tcm.syncing.SqlServerSyncingTool;

/**
 * to create MappingTool by {@link DataSourceEnum}
 * @author bufan
 * @date 2021/8/31
 */
public class MappingToolFactory {

    public static MappingTool create(DataSourceEnum dataSourceEnum){
        switch (dataSourceEnum){
            case MYSQL:
                return new MysqlMappingTool();
            case POSTGRESQL:
                return new PgsqlMappingTool();
            case SQLSERVER:
                return new SqlServerMappingTool();
            default:
                return null;
        }
    }

}
