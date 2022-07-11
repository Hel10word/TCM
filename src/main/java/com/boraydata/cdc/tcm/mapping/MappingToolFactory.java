package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;

/**
 * to create MappingTool by {@link DataSourceEnum}
 * @author bufan
 * @date 2021/8/31
 */
public class MappingToolFactory {

    public static MappingTool create(DataSourceEnum dataSourceEnum){
        switch (dataSourceEnum){
            case MYSQL:
                return new MySQLMappingTool();
            case POSTGRESQL:
                return new PostgreSQLMappingTool();
            case SQLSERVER:
                return new SqlServerMappingTool();
            case RPDSQL:
                return new RpdSqlMappingTool();
            default:
                return null;
        }
    }

}
