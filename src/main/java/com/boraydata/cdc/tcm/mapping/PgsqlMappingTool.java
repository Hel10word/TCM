package com.boraydata.cdc.tcm.mapping;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.common.enums.TCMDataTypeEnum;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.boraydata.cdc.tcm.entity.Table;

import java.util.*;

/**
 * deal with the mapping relationship between PostgreSQL Type and TCM Type
 * @since Fabric CDC V1.0 mainly uses the debezium plugin for parsing binlog log,so refer to part of the design.
 * design by :
 * @see <a href="https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types"></a>
 * All Data Type from official document:
 * @see <a href="https://www.postgresql.org/docs/13/datatype.html">PostgreSQL 13 Data Type</a>
 * @author bufan
 * @date 2021/9/1
 */
public class PgsqlMappingTool implements MappingTool {

    private static final Map<String, TCMDataTypeEnum> mappingMap = new LinkedHashMap<>();
    static {
        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-numeric.html">datatype-numeric</a>
         */
        mappingMap.put("SMALLINT", TCMDataTypeEnum.INT16);
        mappingMap.put("INTEGER", TCMDataTypeEnum.INT32);
        mappingMap.put("BIGINT", TCMDataTypeEnum.INT64);
        mappingMap.put("DECIMAL", TCMDataTypeEnum.DECIMAL);
        mappingMap.put("NUMERIC", TCMDataTypeEnum.DECIMAL);
        mappingMap.put("REAL", TCMDataTypeEnum.FLOAT32);
        mappingMap.put("DOUBLE PRECISION", TCMDataTypeEnum.FLOAT64);
        mappingMap.put("SMALLSERIAL", TCMDataTypeEnum.INT16);
        mappingMap.put("SERIAL", TCMDataTypeEnum.INT32);
        mappingMap.put("BIGSERIAL", TCMDataTypeEnum.INT64);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-money.html">datatype-money</a>
         */
        mappingMap.put("MONEY", TCMDataTypeEnum.DECIMAL);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-character.html">datatype-character</a>
         */
        mappingMap.put("CHARACTER VARYING", TCMDataTypeEnum.STRING);
        mappingMap.put("VARCHAR", TCMDataTypeEnum.STRING);
        mappingMap.put("CHARACTER", TCMDataTypeEnum.STRING);
        mappingMap.put("CHAR", TCMDataTypeEnum.STRING);
        mappingMap.put("TEXT", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-binary.html">datatype-binary</a>
         */
        mappingMap.put("BYTEA", TCMDataTypeEnum.BYTES);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-datetime.html">datatype-datetime</a>
         */
        mappingMap.put("TIMESTAMP", TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("TIMESTAMP WITH TIME ZONE", TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("TIMESTAMP WITHOUT TIME ZONE", TCMDataTypeEnum.TIMESTAMP);
        mappingMap.put("DATE", TCMDataTypeEnum.DATE);
        mappingMap.put("TIME", TCMDataTypeEnum.TIME);
        mappingMap.put("TIME WITH TIME ZONE", TCMDataTypeEnum.TIME);
        mappingMap.put("TIME WITHOUT TIME ZONE", TCMDataTypeEnum.TIME);
        mappingMap.put("INTERVAL", TCMDataTypeEnum.STRING);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-boolean.html">datatype-boolean</a>
         */
        mappingMap.put("BOOLEAN", TCMDataTypeEnum.BOOLEAN);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-enum.html">datatype-enum</a>
         *
         * @since unable support
         */

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-geometric.html">datatype-geometric</a>
         */
        mappingMap.put("POINT", TCMDataTypeEnum.TEXT);
        mappingMap.put("LINE", TCMDataTypeEnum.TEXT);
        mappingMap.put("LSEG", TCMDataTypeEnum.TEXT);
        mappingMap.put("BOX", TCMDataTypeEnum.TEXT);
        mappingMap.put("PATH", TCMDataTypeEnum.TEXT);
        mappingMap.put("POLYGON", TCMDataTypeEnum.TEXT);
        mappingMap.put("CIRCLE", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-net-types.html">datatype-net-types</a>
         */
        mappingMap.put("CIDR", TCMDataTypeEnum.TEXT);
        mappingMap.put("INET", TCMDataTypeEnum.TEXT);
        mappingMap.put("MACADDR", TCMDataTypeEnum.TEXT);
        mappingMap.put("MACADDR8", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-bit.html">datatype-bit</a>
         */
        mappingMap.put("BIT", TCMDataTypeEnum.BYTES);
        mappingMap.put("BIT VARYING", TCMDataTypeEnum.BYTES);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-textsearch.html">datatype-textsearch</a>
         */
        mappingMap.put("TSVECTOR", TCMDataTypeEnum.TEXT);
        mappingMap.put("TSQUERY", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-uuid.html">datatype-uuid</a>
         */
        mappingMap.put("UUID", TCMDataTypeEnum.STRING);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-xml.html">datatype-xml</a>
         */
        mappingMap.put("XML", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-json.html">datatype-json</a>
         */
        mappingMap.put("JSON", TCMDataTypeEnum.TEXT);
        mappingMap.put("JSONB", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/arrays.html">arrays</a>
         */
        mappingMap.put("ARRAY", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/rowtypes.html">rowtypes</a>
         *
         * @since unable support
         */

        /**
         * @see <a href="https://www.postgresql.org/docs/13/rangetypes.html">rangetypes</a>
         */
        mappingMap.put("INT4RANGE", TCMDataTypeEnum.TEXT);
        mappingMap.put("INT8RANGE", TCMDataTypeEnum.TEXT);
        mappingMap.put("NUMRANGE", TCMDataTypeEnum.TEXT);
        mappingMap.put("TSRANGE", TCMDataTypeEnum.TEXT);
        mappingMap.put("TSTZRANGE", TCMDataTypeEnum.TEXT);
        mappingMap.put("DATERANGE", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/domains.html">domains</a>
         */
        mappingMap.put("OID", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGCLASS", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGCOLLATION", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGCONFIG", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGDICTIONARY", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGNAMESPACE", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGOPER", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGOPERATOR", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGPROC", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGPROCEDURE", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGROLE", TCMDataTypeEnum.TEXT);
        mappingMap.put("REGTYPE", TCMDataTypeEnum.TEXT);


        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-oid.html">datatype-oid</a>
         */
        mappingMap.put("PG_LSN", TCMDataTypeEnum.TEXT);

        /**
         * @see <a href="https://www.postgresql.org/docs/13/datatype-pg-lsn.html">datatype-pg-lsn</a>
         *
         * @since unable support
         */

    }


    @Override
    public Table createSourceMappingTable(Table table) {
        if(table.getColumns() == null || table.getColumns().isEmpty())
            throw new TCMException("Table.getColumns() is null or empty \n"+table.outTableInfo());
        List<Column> columns = table.getColumns();
        for (Column column : columns){
            if (StringUtil.isNullOrEmpty(column.getDataType()))
                throw new TCMException("not found DataType value in "+column);
            TCMDataTypeEnum relation = StringUtil.findRelation(mappingMap,column.getDataType(),null);
            if (Objects.isNull(relation)){
                    throw new TCMException("not found DataType relation in "+column);
            }
            String colDataType = StringUtil.dataTypeFormat(column.getDataType());
            if("MONEY".equalsIgnoreCase(colDataType))
                column.setNumericPrecision(1000).setNumericScale(2);
//            if("BOOLEAN".equalsIgnoreCase(colDataType))
//                column.setCharacterMaximumPosition(1L);
            if("INTERVAL".equalsIgnoreCase(colDataType))
                column.setCharacterMaximumPosition(255L);
            if("UUID".equalsIgnoreCase(colDataType))
                column.setCharacterMaximumPosition(255L);

            column.setTCMDataTypeEnum(relation);
        }
        table.setColumns(columns);
        if(table.getDataSourceEnum() == null)
            table.setDataSourceEnum(DataSourceEnum.POSTGRESQL);
        if(table.getOriginalDataSourceEnum() == null)
            table.setOriginalDataSourceEnum(DataSourceEnum.POSTGRESQL);
        return table;
    }


    @Override
    public Table createCloneMappingTable(Table table) {
        Table cloneTable = table.clone();
        if(table.getDataSourceEnum() != null && DataSourceEnum.POSTGRESQL.equals(table.getDataSourceEnum()))
            return cloneTable;
        for (Column col : cloneTable.getColumns())
            col.setDataType(col.getTCMDataTypeEnum().getMappingDataType(DataSourceEnum.POSTGRESQL));
        cloneTable.setDataSourceEnum(DataSourceEnum.POSTGRESQL);
        return cloneTable;
    }


    /**
     *
     * generate the Create Table Statement by table
     * Refer to the official documentation (PgSQL 13)
     * @see <a href="https://www.postgresql.org/docs/13/sql-createtable.html">create-table</a>
     * e.g.
     *   Create Table If Not Exists "schema name"."table name"(
     *      "column name" integer not null,
     *      "column name" varchar not null,
     *      ....
     *   );
     *
     * @Param Table : table={null,PGSQL,test_table,columns={null,null,col_int,DataType=INT,dataTypeMapping=INT32}}
     * @Return: String
     */
    @Override
    public String getCreateTableSQL(Table table) {
        if(table.getTableName() == null)
            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceEnum().name());
        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists ");
        if(table.getSchemaName() == null)
            stringBuilder.append("\"").append(table.getTableName()).append("\"(\n");
        else
            stringBuilder.append(table.getSchemaName()).append(".\"").append(table.getTableName()).append("\"(\n");
        List<Column> columns = table.getColumns();
        for(Column column : columns){
            TCMDataTypeEnum tcmDataTypeEnum = column.getTCMDataTypeEnum();
            String dataType = column.getDataType();
            if(StringUtil.isNullOrEmpty(dataType) && Objects.isNull(tcmDataTypeEnum))
                throw new TCMException("Create Table SQL is fail,Because unable use null datatype:"+column);
            if(StringUtil.isNullOrEmpty(dataType))
                dataType = tcmDataTypeEnum.getMappingDataType(DataSourceEnum.POSTGRESQL);

            stringBuilder.append("\"").append(column.getColumnName()).append("\" ").append(dataType);


            if(TCMDataTypeEnum.STRING.equals(tcmDataTypeEnum)){
                Long characterMaximumPosition = column.getCharacterMaximumPosition();
                if(characterMaximumPosition != null && characterMaximumPosition != 0) {
                    if (0 <= characterMaximumPosition && characterMaximumPosition <= 10485760)
                        stringBuilder.append("(").append(characterMaximumPosition).append(")");
                    else
                        stringBuilder.append("(10485760)");
                }
            } else if(
                    TCMDataTypeEnum.TIME.equals(tcmDataTypeEnum) ||
                    TCMDataTypeEnum.TIMESTAMP.equals(tcmDataTypeEnum)
            ){
                Integer datetimePrecision = column.getDatetimePrecision();
                if(datetimePrecision != null && datetimePrecision != 0) {
                    if(0 < datetimePrecision && datetimePrecision <= 6)
                        stringBuilder.append("(").append(datetimePrecision).append(")");
                    else
                        stringBuilder.append("(6)");
                    stringBuilder.append(" with time zone ");
                }
            } else if(TCMDataTypeEnum.DECIMAL.equals(tcmDataTypeEnum)){
                Integer precision = column.getNumericPrecision();
                Integer scale = column.getNumericScale();
                if(precision != null && precision != 0){
                    if(1 <= precision && precision <= 1000)
                        stringBuilder.append("(").append(precision);
                    else {
                        precision = 1000;
                        stringBuilder.append("(1000");
                    }
                    if(scale != null && scale != 0){
                        if( 0 <= scale && scale <= precision)
                            stringBuilder.append(",").append(scale);
                        else
                            stringBuilder.append(",").append(precision);
                    }
                    stringBuilder.append(")");
                }
            }else {
                // Nothing to do
            }
            if (Boolean.FALSE.equals(column.getNullable()))
                stringBuilder.append(" not NULL");
            stringBuilder.append("\n,");
        }
        List<String> primaryKeys = table.getPrimaryKeys();
        if(Objects.nonNull(primaryKeys) && Boolean.FALSE.equals(primaryKeys.isEmpty()))
            stringBuilder.append("PRIMARY KEY(").append(String.join(",", primaryKeys)).append(")\n,");

        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        stringBuilder.append(");");
        return stringBuilder.toString();
    }
}
