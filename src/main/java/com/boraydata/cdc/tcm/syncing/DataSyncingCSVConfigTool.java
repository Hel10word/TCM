package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author : bufan
 * @date : 2022/6/16
 *
 *                  Delimiter       LineSeparate        Quote       Escape
 * Default              |               \n              "               \
 * MySQL                √               √               √               √
 * PostgreSQL           √               x               √               √
 * SQL Server           √               √               x               x
 *
 *
 */
public class DataSyncingCSVConfigTool {
    private static final Logger logger = LoggerFactory.getLogger(DataSyncingCSVConfigTool.class);
//    private static final String SQL_SERVER_DELIMITER = "\u0007";
//    private static final String SQL_SERVER_DELIMITER = "\007";
    public static final String SQL_SERVER_DELIMITER_7 = String.valueOf((char) 7);
//    private static final String SQL_SERVER_DELIMITER = "CHR(7)";
    /**
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
     */
    public static void CompleteContext(TableCloneManagerContext context){
        DataSourceEnum sourceEnum = context.getSourceConfig().getDataSourceEnum();
        DataSourceEnum cloneEnum = context.getCloneConfig().getDataSourceEnum();
        switch (sourceEnum){
            case MYSQL:
                // Nothing to do
                break;
            case POSTGRESQL:
                break;
            case SQLSERVER:
                context.getTcmConfig().setDelimiter(SQL_SERVER_DELIMITER_7);
                context.getTcmConfig().setEscape(null);
                context.getTcmConfig().setQuote(null);
                break;
            default:
                break;
        }

        switch (cloneEnum){
            case MYSQL:
//                context.getTcmConfig().setQuote(null);
                // Nothing to do
                break;
            case POSTGRESQL:
//                context.getTcmConfig().setQuote(null);
                break;
            case SQLSERVER:
                context.getTcmConfig().setDelimiter(SQL_SERVER_DELIMITER_7);
                context.getTcmConfig().setEscape(null);
                context.getTcmConfig().setQuote(null);
                break;
            case HUDI:
                context.getTcmConfig().setQuote("\"");
                context.getTcmConfig().setEscape("\\");
                break;
            default:
                break;
        }
    }

    /**
     *  because Hudi Data storage in Hive,Hive Data Type not support Boolean(0,1,"t","f"),
     *  just allow ('true','false'),so should mapping this data by select.
     * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"></a>
     */

}
