package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Column;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.syncing.util.SqlServerIndexTool;
import com.boraydata.cdc.tcm.utils.StringUtil;

import java.util.Objects;

import static com.boraydata.cdc.tcm.utils.StringUtil.escapeJava;


/**
 * Export and Load Table Data by SQL Server
 *
 * @author : bufan
 * @date : 2022/6/11
 */
public class SqlServerSyncingTool implements SyncingTool {
    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
        return generateExportSQLByBCP(tcmContext);
    }



    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
        return generateLoadSQLByBCP(tcmContext);
    }


    @Override
    public Boolean executeExport(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
    }

    @Override
    public Boolean executeLoad(TableCloneManagerContext tcmContext) {

        if(Boolean.FALSE.equals(SqlServerIndexTool.disableIndex(tcmContext)))
            return false;

        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());

        if(Boolean.FALSE.equals(SqlServerIndexTool.rebuildIndex(tcmContext)))
            return false;

        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
    }




    // =================================================================  BCP  =============================================================================
    /**
     * The BCP (Bulk Copy Program) utility is a command line that program
     * that bulk-copies data between a SQL instance and a data file using a special format file.
     * The BCP utility can be used to import large numbers of rows into SQL Server or export SQL Server data into files.
     * @see <a href="https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-linux-ver15"></a>
     *
     * have a question,bcp export csv unsupport escape or quotes
     * @see <a href="https://stackoverflow.com/questions/1976086/getting-bcp-exe-to-escape-terminators"></a>
     * @see <a href="https://stackoverflow.com/questions/2061113/sql-server-bcp-how-to-put-quotes-around-all-fields"></a>
     * unable support Escape by official
     * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-and-row-terminators-sql-server?view=sql-server-linux-ver15#characters-supported-as-terminators"></a>
     */


    /**
     * bcp dbo.lineitem out /opt/export.csv -S 127.0.0.1,1433 -U sa -P 123 -d test_db -c -a 4096
     * @return bcp ? -S localhost,port -U username -P password -d databasesName -c -a 4096
     * -t delimiter
     * -r lineSeparate
     */
    private String getBCPConnectCommand(DatabaseConfig config) {
        return String.format("bcp ? -S %s,%s -U %s -P %s -d %s -c -a 4096 ",
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabaseName());
    }

    /**
     * @author : bufan
     * @return : bcp dbo.lineitem out /opt/split_files/export.csv -S localhost -U sa -PRapids123* -d flowtest -c -t '|' -r '\n' -a 4096
     */
    public String generateExportSQLByBCP(TableCloneManagerContext tcmContext) {

        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
//        boolean hudiFlag = DataSourceEnum.HUDI.equals(tcmContext.getCloneConfig().getDataSourceEnum());

        /**
         *  because Hudi Data storage in Hive,Hive Data Type not support Boolean(0,1,"t","f"),
         *  just allow ('true','false'),so should mapping this data by select.
         * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"></a>
         */
        String tableName = "";
        if(Boolean.FALSE.equals(Objects.isNull(tempTable)))
            tableName = tcmContext.getTempTableSelectSQL();
        else
            tableName = "select * from "+tcmContext.getSourceConfig().getSchema()+"."+sourceTable.getTableName();

        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String bcpConnectCommand = getBCPConnectCommand(tcmContext.getSourceConfig());
//        String exportContent = replaceExportStatementShellByOut(bcpConnectCommand,tableName,csvPath,delimiter,hudiFlag);
        String exportContent = replaceExportStatementShell(bcpConnectCommand,sourceTable,tableName,csvPath,delimiter,lineSeparate,quote,escape);

        /**
         * echo -e "aaa \r\n cccc" | sed -r 's/\r/bbbb/g'
         * echo -e "aaa \r\n cccc" | sed -r ":x;N;s/\r\n/\\\r\\\n/g;bx"
         * @see <a href="https://www.gnu.org/software/sed/manual/sed.html#sed-commands-list"></a>
         * @see <a href="https://www.gnu.org/software/sed/manual/sed.html#Branching-and-flow-control"></a>
         */
        if(DataSourceEnum.POSTGRESQL.equals(tcmContext.getCloneConfig().getDataSourceEnum()))
            exportContent = exportContent+"\n"+
                    "sed -r -i \":x;N;s/\\r\\n/\\\\\\r\\\\\\n/g;bx\" "+csvPath;

        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }


    /**
     * @see <a href="https://docs.microsoft.com/zh-cn/sql/tools/bcp-utility?view=sql-server-linux-ver15#g-copying-data-from-a-query-to-a-data-file"></a>
     */
    private String replaceExportStatementShell(String con,Table table, String tableName, String filePath, String delimiter,String lineSeparate,String quote,String escape){

        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);

        /**
         * with t1 as(
         * 	select l_boolean,booleanMapping = iif(l_boolean = 1,'True','False') from test_table
         * )
         * select QUOTENAME(booleanMapping ,'"') from t1
         * @see <a href="https://docs.microsoft.com/en-us/sql/t-sql/functions/logical-functions-iif-transact-sql?view=sql-server-linux-ver15"></a>
         */
        if (StringUtil.nonEmpty(quote)){
            String templeTable = "table_"+StringUtil.getRandom();
            StringBuilder tableQuery = new StringBuilder("with ").append(templeTable).append(" as(").append(tableName).append(") select ");
            for (Column col : table.getColumns())
                tableQuery.append("QUOTENAME(").append(col.getColumnName()).append(",'").append(escapeJava(quote,"'")).append("'),");
            if(tableQuery.lastIndexOf(",") == tableQuery.length()-1)
                tableQuery.deleteCharAt(tableQuery.length()-1);
            tableName = tableQuery.append(" from ").append(templeTable).toString();
        }
        StringBuilder stringBuilder = new StringBuilder(con.replace("?","\""+escapeJava(tableName,"\"")+"\" queryout '"+escapeJava(filePath,"'")+"'"));
        if(StringUtil.nonEmpty(delimiter)) {
//            stringBuilder.append(" -t '").append(delimiter).append("'");
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                stringBuilder.append("-t '0x07'");
            else
                stringBuilder.append(" -t '").append(escapeJava(delimiter,"'")).append("'");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            stringBuilder.append(" -r '").append(escapeJava(lineSeparate,"'")).append("'");

        return stringBuilder.toString();
    }

    public String generateLoadSQLByBCP(TableCloneManagerContext tcmContext) {
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneConfig().getSchema()+"."+tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String lineSeparate = tcmContext.getTcmConfig().getLineSeparate();
        String quote = tcmContext.getTcmConfig().getQuote();
        String escape = tcmContext.getTcmConfig().getEscape();
        String bcpConnectCommand = getBCPConnectCommand(tcmContext.getCloneConfig());
//        String loadContent = replaceLoadStatementShell(bcpConnectCommand, csvPath,tableName,delimiter);
        String loadContent = replaceLoadStatementShell(bcpConnectCommand,tableName,csvPath,delimiter,lineSeparate,quote,escape);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }


    private String replaceLoadStatementShell(String con,String tableName, String filePath, String delimiter,String lineSeparate,String quote,String escape) {
        tableName = escapeJava(tableName);
        delimiter = escapeJava(delimiter);
        lineSeparate = escapeJava(lineSeparate);
        quote = escapeJava(quote);
        escape = escapeJava(escape);

        StringBuilder stringBuilder = new StringBuilder(con.replace("?","\""+escapeJava(tableName,"\"")+"\" in '"+filePath+"'"));
        if(StringUtil.nonEmpty(delimiter)) {
            if(escapeJava(DataSyncingCSVConfigTool.SQL_SERVER_DELIMITER_7).equals(delimiter))
                stringBuilder.append("-t '0x07'");
            else
                stringBuilder.append(" -t '").append(escapeJava(delimiter,"'")).append("'");
        }
        if(StringUtil.nonEmpty(lineSeparate))
            stringBuilder.append(" -r '").append(escapeJava(lineSeparate,"'")).append("'");

        return stringBuilder.toString();
    }

}
