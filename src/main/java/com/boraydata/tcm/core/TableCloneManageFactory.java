package com.boraydata.tcm.core;

import com.boraydata.tcm.syncing.SyncingTool;
import com.boraydata.tcm.syncing.SyncingToolFactory;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.mapping.MappingTool;
import com.boraydata.tcm.mapping.MappingToolFactory;

/** Use factory mode initialization TCM
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageFactory {
    private TableCloneManageFactory() {throw new IllegalStateException("Utility class");}
    public static TableCloneManage createTableCloneManage(TableCloneManageContext tableCloneManageContext){
        TableCloneManage tableCloneManage;
        DataSourceType sourceType = tableCloneManageContext.getSourceConfig().getDataSourceType();
        DataSourceType cloneType = tableCloneManageContext.getCloneConfig().getDataSourceType();
        if(sourceType != null && cloneType != null) {
            MappingTool sourceMappingTool = MappingToolFactory.create(sourceType);
            MappingTool cloneMappingTool = MappingToolFactory.create(cloneType);
            SyncingTool sourceCommand = SyncingToolFactory.create(sourceType);
            SyncingTool cloneCommand = SyncingToolFactory.create(cloneType);
            tableCloneManage = new TableCloneManage(
                    tableCloneManageContext.getSourceConfig(),
                    tableCloneManageContext.getCloneConfig(),
                    sourceMappingTool,
                    cloneMappingTool,
                    sourceCommand,
                    cloneCommand,
                    tableCloneManageContext.getAttachConfig()
            );
        }else {
            throw new TCMException("should write complete Config in   *.tcm.core.TableCloneManageFactory.createTableCloneManage");
        }
        return tableCloneManage;
    }
}
