package com.boraydata.tcm.command;

import com.boraydata.tcm.core.DataSourceType;

/** to create sqlCommandGenerate
 * @author bufan
 * @data 2021/9/26
 */
public class CommandGenerateFactory {
    public static CommandGenerate create(DataSourceType type){
        if (type == DataSourceType.MYSQL)
            return new MysqlCommandGenerate();
        else if (type == DataSourceType.POSTGRES)
            return new PgsqlCommandGenerate();
        return null;
    }
}
