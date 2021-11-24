package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.AttachConfig;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;

/** Initiates the connection information of the data sources
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageContext {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;
    private AttachConfig attachConfig;

    TableCloneManageContext(Builder builder){
        this.sourceConfig = builder.sourceConfig;
        this.cloneConfig = builder.cloneConfig;
        this.attachConfig = builder.attachConfig;
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public AttachConfig getAttachConfig() {
        return attachConfig;
    }

    public static class Builder{
        private DatabaseConfig sourceConfig;
        private DatabaseConfig cloneConfig;
        private AttachConfig attachConfig;

        public Builder() {
        }

        public Builder setSourceConfig(DatabaseConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public Builder setCloneConfig(DatabaseConfig cloneConfig) {
            this.cloneConfig = cloneConfig;
            return this;
        }

        public Builder setAttachConfig(AttachConfig attachConfig) {
            this.attachConfig = attachConfig;
            return this;
        }

        public TableCloneManageContext create() {
            if(sourceConfig == null || cloneConfig == null || attachConfig == null)
                throw new TCMException(" sourceConfig、cloneConfig、attachConfig unable null！！！ ");
            if (DataSourceType.HUDI.toString().equals(sourceConfig.getDataSourceType().toString()))
                throw new TCMException(" now does not support Hive to DB ");
            if(StringUtil.isNullOrEmpty(attachConfig.getSourceTableName()))
                throw new TCMException(" 'AttachConfig.sourceTable' is null");
            if(StringUtil.isNullOrEmpty(attachConfig.getCloneTableName()))
                throw new TCMException(" 'AttachConfig.cloneTable' is null");
            if(StringUtil.isNullOrEmpty(attachConfig.getTempDirectory()))
                throw new TCMException(" 'AttachConfig.tempDirectory' is null");
            if (DataSourceType.HUDI.toString().equals(cloneConfig.getDataSourceType().toString())){
                if(StringUtil.isNullOrEmpty(attachConfig.getHdfsCsvDir()))
                    throw new TCMException("if you want use DB-Hive,the 'HdfsCsvDir' You need to fill in");
                if(StringUtil.isNullOrEmpty(attachConfig.getHiveHdfsPath()))
                    throw new TCMException("if you want use DB-Hive,the 'HiveHdfsPath' You need to fill in");
                if(StringUtil.isNullOrEmpty(attachConfig.getHivePrimaryKey()))
                    throw new TCMException("if you want use DB-Hive,the 'HivePrimaryKey' You need to fill in");
                if(StringUtil.isNullOrEmpty(attachConfig.getHivePartitionKey()))
                    throw new TCMException("if you want use DB-Hive,the 'HivePartitionKey' You need to fill in");
            }
            return new TableCloneManageContext(this);
        }
    }

}
