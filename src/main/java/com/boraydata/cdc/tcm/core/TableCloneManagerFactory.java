package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.syncing.SyncingToolFactory;
import com.boraydata.cdc.tcm.syncing.SyncingTool;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;

/**
 * Use factory mode initialization {@link TableCloneManager}
 * @author bufan
 * @date 2021/8/25
 */
public class TableCloneManagerFactory {
    private TableCloneManagerFactory() {throw new IllegalStateException("Utility class");}
    public static TableCloneManager createTableCloneManage(TableCloneManagerContext tableCloneManagerContext){
        TableCloneManager tableCloneManager;
        DataSourceEnum sourceType = tableCloneManagerContext.getSourceConfig().getDataSourceEnum();
        DataSourceEnum cloneType = tableCloneManagerContext.getCloneConfig().getDataSourceEnum();
        if(sourceType != null && cloneType != null) {
            MappingTool sourceMappingTool = MappingToolFactory.create(sourceType);
            MappingTool cloneMappingTool = MappingToolFactory.create(cloneType);
            SyncingTool sourceCommand = SyncingToolFactory.create(sourceType);
            SyncingTool cloneCommand = SyncingToolFactory.create(cloneType);
            tableCloneManager = new TableCloneManager(
                    sourceMappingTool,
                    cloneMappingTool,
                    sourceCommand,
                    cloneCommand,
                    tableCloneManagerContext
            );
        }else {
            throw new TCMException("should write complete Config in   *.tcm.core.TableCloneManagerFactory.createTableCloneManager");
        }
        return tableCloneManager;
    }
}
