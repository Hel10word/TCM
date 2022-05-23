package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;

/**
 * to create MappingTool by {@link DataSourceEnum}
 * @author bufan
 * @data 2021/8/31
 */
public class MappingToolFactory {

    public static MappingTool create(DataSourceEnum dataSourceEnum){
        if(DataSourceEnum.MYSQL.toString().equals(dataSourceEnum.name()))
            return new MysqlMappingTool();
        else if(DataSourceEnum.POSTGRESQL.toString().equals(dataSourceEnum.name()))
            return new PgsqlMappingTool();
        return null;
    }

}
