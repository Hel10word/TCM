package com.boraydata.tcm.core;

import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.mapping.MappingToolFactory;


/** Use factory mode initialization TCM
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageFactory {
    public static TableCloneManage createTableCloneManage(TableCloneManageContext tableCloneManageContext){
        TableCloneManage tableCloneManage = null;
        if(tableCloneManageContext.getSourceConfig() != null && tableCloneManageContext.getCloneConfig() != null) {
            MappingTool SourceMappingTool = MappingToolFactory.create(tableCloneManageContext.getSourceConfig().getDataSourceType());
            MappingTool CloneMappingTool = MappingToolFactory.create(tableCloneManageContext.getCloneConfig().getDataSourceType());
            tableCloneManage = new TableCloneManage(
                    tableCloneManageContext.getSourceConfig(),
                    tableCloneManageContext.getCloneConfig(),
                    SourceMappingTool,
                    CloneMappingTool
            );
        }else {
            throw new TCMException("should write complete Config in   *.tcm.core.TableCloneManageFactory.createTableCloneManage");
        }
        return tableCloneManage;
    }
}
