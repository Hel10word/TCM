package com.boraydata.tcm.mapping;

import com.boraydata.tcm.core.DataSourceType;

/** to create MappingTool by DataSourceType
 * @author bufan
 * @data 2021/8/31
 */
public class MappingToolFactory {

    public static MappingTool create(DataSourceType dataSourceType){
        if(DataSourceType.MYSQL.name().equals(dataSourceType.name()))
            return new MysqlMappingTool();
        if(DataSourceType.POSTGRES.name().equals(dataSourceType.name()))
            return new PgsqlMappingTool();
        return null;
    }

}
