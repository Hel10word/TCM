package com.boraydata.tcm.core;

import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.exception.TCMException;

/** Initiates the connection information of the data sources
 * @author bufan
 * @data 2021/8/25
 */
public class TableCloneManageContext {

    private DatabaseConfig sourceConfig;
    private DatabaseConfig cloneConfig;

    TableCloneManageContext(Builder builder){
        this.sourceConfig = builder.sourceConfig;
        this.cloneConfig = builder.cloneConfig;
    }

    public DatabaseConfig getSourceConfig() {
        return sourceConfig;
    }

    public DatabaseConfig getCloneConfig() {
        return cloneConfig;
    }

    public static class Builder{
        private DatabaseConfig sourceConfig;
        private DatabaseConfig cloneConfig;

        public Builder() {
        }

        public Builder setSourceConfig(DatabaseConfig sourceConfig) {
            if (DataSourceType.SPARK.toString().equals(sourceConfig.getDataSourceType().toString()))
                throw new TCMException(" now does not support Spark to DB ");
            this.sourceConfig = sourceConfig;
            return this;
        }

        public Builder setCloneConfig(DatabaseConfig cloneConfig) {
            this.cloneConfig = cloneConfig;
            return this;
        }

        public TableCloneManageContext create() {
            return new TableCloneManageContext(this);
        }
    }

}
