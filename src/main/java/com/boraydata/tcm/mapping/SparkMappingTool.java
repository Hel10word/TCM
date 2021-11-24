//package com.boraydata.tcm.mapping;
//
//import com.boraydata.tcm.core.DataSourceType;
//import com.boraydata.tcm.entity.Column;
//import com.boraydata.tcm.entity.Table;
//import com.boraydata.tcm.exception.TCMException;
//
//import java.util.LinkedList;
//import java.util.List;
//
///** deal with the mapping relationship between Spark Type and TCM Type
// * https://spark.apache.org/docs/3.1.2/sql-ref-datatypes.html
// *
// * @author bufan
// * @data 2021/10/12
// */
//public class SparkMappingTool implements MappingTool {
//
////    Spark to TCM Type
//    @Override
//    public Table createSourceMappingTable(Table table) {
//        return null;
//    }
//
////    TCM Type to Spark
//    @Override
//    public Table createCloneMappingTable(Table table) {
//        return createCloneMappingTable(table,table.getTablename());
//    }
//
//    @Override
//    public Table createCloneMappingTable(Table table, String tableName) {
//        Table cloneTable = table.clone();
//        List<Column> sourceCols = cloneTable.getColumns();
//        List<Column> cloneCols = new LinkedList<>();
//        for (Column col : sourceCols){
//            Column c = col.clone();
//            c.setDataType(col.getTableCLoneManageType().getOutDataType(DataSourceType.SPARK));
//            cloneCols.add(c);
//        }
//        cloneTable.setCatalogname(null);
//        cloneTable.setSchemaname(null);
//        cloneTable.setColumns(cloneCols);
//        cloneTable.setDataSourceType(DataSourceType.SPARK);
//        cloneTable.setTablename(tableName);
//        return cloneTable;
//    }
//
//    @Override
//    public String getCreateTableSQL(Table table) {
//        if(table.getTablename() == null)
//            throw new TCMException("Failed in create table SQL,Because ‘Table.TableName’ is null. You should set one ."+table.getDataSourceType().name());
//        StringBuilder stringBuilder = new StringBuilder("Create Table If Not Exists "+table.getTablename()+"(\n");
//        List<Column> columns = table.getColumns();
//        for(Column column : columns){
//            if(column.getDataType() == null)
//                throw new TCMException("Create Table SQL is fail,Because unable use null type:"+column.getColumnInfo());
//            stringBuilder.append(column.getColumnName()).append(" ").append(column.getDataType());
//            if (Boolean.FALSE.equals(column.isNullAble()))
//                stringBuilder.append(" not NULL");
//            stringBuilder.append("\n,");
//        }
//        stringBuilder.deleteCharAt(stringBuilder.length()-1);
//        stringBuilder.append(");");
//        return stringBuilder.toString();
//    }
//}
