package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.syncing.SyncingToolFactory;
import com.boraydata.cdc.tcm.syncing.SyncingTool;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;

/**
 * Use factory mode initialization {@link TableCloneManage}
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageFactory {
    private TableCloneManageFactory() {throw new IllegalStateException("Utility class");}
    public static TableCloneManage createTableCloneManage(TableCloneManageContext tableCloneManageContext){
        TableCloneManage tableCloneManage;
        DataSourceEnum sourceType = tableCloneManageContext.getSourceConfig().getDataSourceEnum();
        DataSourceEnum cloneType = tableCloneManageContext.getCloneConfig().getDataSourceEnum();
        if(sourceType != null && cloneType != null) {
            MappingTool sourceMappingTool = MappingToolFactory.create(sourceType);
            MappingTool cloneMappingTool = MappingToolFactory.create(cloneType);
            SyncingTool sourceCommand = SyncingToolFactory.create(sourceType);
            SyncingTool cloneCommand = SyncingToolFactory.create(cloneType);
            tableCloneManage = new TableCloneManage(
                    sourceMappingTool,
                    cloneMappingTool,
                    sourceCommand,
                    cloneCommand,
                    tableCloneManageContext
            );
        }else {
            throw new TCMException("should write complete Config in   *.tcm.core.TableCloneManageFactory.createTableCloneManage");
        }
        return tableCloneManage;
    }
}
