package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.entity.Table;

import java.util.Objects;


/**
 * Export and Load Table Data by SQL Server
 *
 * hudiFlag:
 *      Spark parsing CSV If you need to use Escape characters, you need to enclose the field,
 *      that is, you can use Escape only in combination with the Quote attribute.
 * @see <a href="https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html"></a>
 *  e.g.
 *      delimiter:,
 *      escape:\
 *      Source Table Data:{test,test-delimiter:,,test-escape:\,test-quote:"}
 *      default CSV File => test,test-delimiter:\,,test-escape:\\,test-quote:"
 *      But Spark need => "test","test-delimiter:,","test-escape:\\","test-quote:\""
 *
 *      val df = spark.read.option("delimiter", ",").option("escape", "\\").option("quote", "\"").csv(path)
 *      df.show()
 *   each column of Csv File will be wrapped with the content of Quote. If the data contains Escape or Quote
 *    you need to add Escape in front of it to Escape.
 * @author : bufan
 * @date : 2022/6/11
 */
public class SqlServerSyncingTool implements SyncingTool {
    @Override
    public String getExportInfo(TableCloneManagerContext tcmContext) {
        return generateExportSQLByBCP(tcmContext);
//        return null;
    }



    @Override
    public String getLoadInfo(TableCloneManagerContext tcmContext) {
        return generateLoadSQLByBCP(tcmContext);
//        return null;
    }


    @Override
    public Boolean executeExport(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getExportShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
//        return null;
    }
    // todo 导入前禁用索引
    // https://docs.microsoft.com/zh-cn/sql/relational-databases/indexes/clustered-and-nonclustered-indexes-described?view=sql-server-ver16
    @Override
    public Boolean executeLoad(TableCloneManagerContext tcmContext) {
        String outStr = CommandExecutor.executeShell(tcmContext.getTempDirectory(),tcmContext.getLoadShellName(),tcmContext.getTcmConfig().getDebug());
        if(tcmContext.getTcmConfig().getDebug())
            System.out.println(outStr);
        return true;
//        return null;
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
     */


    /**
     * bcp dbo.lineitem out /opt/export.csv -S 127.0.0.1,1433 -U sa -P 123 -d test_db -c -r '\n' -a 4096 -t '|'
     * @return bcp ? -S localhost,port -U username -P password -d databasesName -c -r '\n' -a 4096 -t
     */
    private String getBCPConnectCommand(DatabaseConfig config) {
        return String.format("bcp ? -S %s,%s -U %s -P %s -d %s -c -r '%s' -a 4096 -t ",
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabaseName(),
                "\\n");
    }

    /**
     * @author : bufan
     * @return : bcp dbo.lineitem out /opt/split_files/export.csv -S localhost -U sa -PRapids123* -d flowtest -c -t '|' -r '\n' -a 4096
     */
    private String generateExportSQLByBCP(TableCloneManagerContext tcmContext) {

        Table tempTable = tcmContext.getTempTable();
        Table sourceTable = tcmContext.getSourceTable();
        boolean hudiFlag = DataSourceEnum.HUDI.equals(tcmContext.getCloneConfig().getDataSourceEnum());

        /**
         *  because Hudi Data storage in Hive,Hive Data Type not support Boolean(0,1,"t","f"),
         *  just allow ('true','false'),so should mapping this data by select.
         * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"></a>
         */
        String tableName = "";
        if(Boolean.FALSE.equals(Objects.isNull(tempTable)))
            tableName = "\""+tcmContext.getTempTableSelectSQL()+"\"";
        else
            tableName = tcmContext.getSourceConfig().getSchema()+"."+sourceTable.getTableName();

        String csvPath = "./"+tcmContext.getCsvFileName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String bcpConnectCommand = getBCPConnectCommand(tcmContext.getSourceConfig());
        String exportContent = replaceExportStatementShellByOut(bcpConnectCommand,tableName,csvPath,delimiter,hudiFlag);
        tcmContext.setExportShellContent(exportContent);
        return exportContent;
    }


    /**
     * @param tableName dbo.lineitem
     * @param com bcp ? -S localhost,port -U username -P password -d databasesName -c -r '\n' -a 4096 -t
     * @return bcp dbo.lineitem out /opt/export.csv -S 127.0.0.1,1433 -U sa -P 123 -d test_db -c -r '\n' -a 4096 -t '|'
     */
    private String replaceExportStatementShellByOut(String com,String tableName,String filePath, String delimiter,boolean hudiFlag) {
        if(hudiFlag)
            return com.replace("?",tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"' CSV QUOTE '\\\"' escape '\\\\' force quote *;");
        else
            return com.replace("?",tableName+" out '"+filePath+"'")+"'"+delimiter+"'";
    }
    /**
     * @param tableName "select * from lineitem"
     * @param com bcp ? -S localhost,port -U username -P password -d databasesName -c -r '\n' -a 4096 -t
     * @return bcp "select * from dbo.lineitem" queryout /opt/export.csv -S 127.0.0.1,1433 -U sa -P 123 -d test_db -c -r '\n' -a 4096 -t '|'
     */
    private String replaceExportStatementShellByQueryout(String com,String tableName,String filePath, String delimiter,boolean hudiFlag) {
        if(hudiFlag)
            return com.replace("?",tableName+" to " + "'"+filePath+"' with DELIMITER '"+delimiter+"' CSV QUOTE '\\\"' escape '\\\\' force quote *;");
        else
            return com.replace("?",tableName+" queryout '"+filePath+"'")+"'"+delimiter+"'";
    }

    private String generateLoadSQLByBCP(TableCloneManagerContext tcmContext) {
        String csvPath = "./"+tcmContext.getCsvFileName();
        String tableName = tcmContext.getCloneConfig().getSchema()+"."+tcmContext.getCloneTable().getTableName();
        String delimiter = tcmContext.getTcmConfig().getDelimiter();
        String bcpConnectCommand = getBCPConnectCommand(tcmContext.getCloneConfig());
        String loadContent = replaceLoadStatementShell(bcpConnectCommand, csvPath,tableName,delimiter);
        tcmContext.setLoadShellContent(loadContent);
        return loadContent;
    }

    private String replaceLoadStatementShell(String com,String filePath,String tableName,String delimiter) {
        return com.replace("?",tableName+" in '"+filePath+"'")+"'"+delimiter+"'";
    }

}
