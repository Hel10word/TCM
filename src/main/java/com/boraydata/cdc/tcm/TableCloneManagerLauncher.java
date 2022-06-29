package com.boraydata.cdc.tcm;

import com.boraydata.cdc.tcm.common.ConfigurationLoader;
import com.boraydata.cdc.tcm.common.Message;
import com.boraydata.cdc.tcm.common.TableCloneManagerConfig;
import com.boraydata.cdc.tcm.core.TableCloneManager;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.core.TableCloneManagerFactory;
import com.boraydata.cdc.tcm.exception.TCMException;
import com.boraydata.cdc.tcm.entity.Table;
import com.boraydata.cdc.tcm.utils.JacksonUtil;
import com.boraydata.cdc.tcm.utils.RedisOperateUtil;
import com.boraydata.cdc.tcm.utils.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;

/** TCM - Table Clone Manager and Table Data Sync
 * @author bufan
 * @date 2021/9/28
 */
public class TableCloneManagerLauncher {
    private static final Logger logger = LoggerFactory.getLogger(TableCloneManagerLauncher.class);

    public static void main(String[] args) throws JsonProcessingException {

        Boolean deleteFlag;
        Boolean debugFlag;
        String sourceTableName;
        String cloneTableName;
        String sourceDataType;
        String cloneDataType;

        TableCloneManagerConfig config;


        String configFilePath = "./test.properties";

        if(args != null && args.length != 0)
            configFilePath = args[0];

        try {
            config = ConfigurationLoader.loadConfigFile(configFilePath);
            deleteFlag = config.getDeleteCache();
            debugFlag = config.getDebug();
            sourceTableName = config.getSourceTableName();
            cloneTableName = config.getCloneTableName();
            sourceDataType = config.getSourceConfig().getDataSourceEnum().toString();
            cloneDataType = config.getCloneConfig().getDataSourceEnum().toString();
        } catch (IOException e) {
            throw new TCMException("make sure the config file exist",e);
        }
        LinkedList<String> sourceTableNames = config.getSourceTableNames();
        LinkedList<String> cloneTableNames = config.getCloneTableNames();
        LinkedList<Table> customTables = config.getCustomTables();
        LinkedList<Message> messageList = new LinkedList<>();
        if(null != cloneTableNames) {
            for (int i = 0; i < sourceTableNames.size(); i++) {
                sourceTableName = sourceTableNames.get(i);
                cloneTableName = cloneTableNames.get(i);
                if(i <= customTables.size()) {
                    Table customTable = customTables.get(i);
                    if (StringUtil.nonEmpty(customTable.getTableName()) && customTable.getTableName().equals(sourceTableName))
                        config.setCustomTable(customTable);
                }
                config.setSourceTableName(sourceTableName);
                config.setCloneTableName(cloneTableName);
                Message message = tableCloneManager(config, deleteFlag, debugFlag, sourceTableName, cloneTableName, sourceDataType, cloneDataType);
                messageList.add(message);
            }
        }else {
            Message message = tableCloneManager(config, deleteFlag, debugFlag, sourceTableName, cloneTableName, sourceDataType, cloneDataType);
            messageList.add(message);
        }
        if(messageList.size()==1)
            RedisOperateUtil.sendMessage(config.getRedis(),JacksonUtil.toJson(messageList.get(0)));
        else
            RedisOperateUtil.sendMessage(config.getRedis(),JacksonUtil.toJson(messageList));


    }

    private static Message tableCloneManager(TableCloneManagerConfig config, Boolean deleteFlag, Boolean debugFlag, String sourceTableName, String cloneTableName, String sourceDataType, String cloneDataType) throws JsonProcessingException {
        Message message = new Message();
        long start = 0;
        long end = 0;
        long all = 0;
        if(Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Source Config Info ==================================\n" +
                    "{}" +
                    "\n================================== Clone Config Info ==================================\n" +
                    "{}\n",config.getSourceConfig().outInfo(),config.getCloneConfig().outInfo());

//====================== 2. Create TableCloneManagerContext, pass the read information to context
        TableCloneManagerContext tcmContext = new TableCloneManagerContext.Builder()
                .setTcmConfig(config)
                .create();

//====================== 3、 use Context create TCM
        TableCloneManager tcm = TableCloneManagerFactory.createTableCloneManage(tcmContext);

        StringBuilder sb = new StringBuilder()
                .append("Source Table : ").append(config.getSourceTableName()).append("\n")
                .append("Source Data : ").append(JacksonUtil.toJson(config.getSourceConfig())).append("\n")
                .append("Clone Table : ").append(config.getCloneTableName()).append("\n")
                .append("Clone Data : ").append(JacksonUtil.toJson(config.getCloneConfig())).append("\n")
                ;
        message.setJobInfo(sb.toString());


        start = System.currentTimeMillis();
        logger.info("Ready to get Source Table metadata information,Table Name:'{}'  Datasource Type:{}",sourceTableName,sourceDataType);
//====================== 4、 get Source Table Struct by tableName in sourceData
        Table sourceTable = tcm.createSourceMappingTable(sourceTableName);
        end = System.currentTimeMillis();
        Long getSourceTable = end-start;
        all+=getSourceTable;
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> get Source Table info total time spent:{}",getSourceTable);
        if (Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Source Table Info ==================================\n" +
                    "{}",sourceTable.outTableInfo());




        start = System.currentTimeMillis();
        logger.info("Ready to create Clone Table,Table Name:'{}'  Datasource Type:{}",cloneTableName,cloneDataType);
//====================== 5、 get Clone Table based on Source Table
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneTableName);
//====================== 6、create Clone Table in clone database
        tcm.createTableInDatasource();
        end = System.currentTimeMillis();
        Long createCloneTable = end-start;
        all+=createCloneTable;
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> create Clone Table total time spent:{}",createCloneTable);
        if (Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Clone Table Info ==================================\n" +
                    "{}",cloneTable.outTableInfo());



        start = System.currentTimeMillis();
        logger.info("*********************************** EXPORT INFO ***********************************");

//====================== 7、create export data script,and execute export shell.
        tcm.exportTableData();
        end = System.currentTimeMillis();
        Long exportFromSource = end-start;
        all+=exportFromSource;
        Table exportTable = Objects.isNull(tcmContext.getTempTable())?tcmContext.getSourceTable():tcmContext.getTempTable();
        logger.info("Export data situation:\nDatasource Type:{}\nTable Name:'{}'\nTable Column Size:{}\nExport Script Path:{}\nExport CSV File Path:{}",
                sourceDataType,
                exportTable.getTableName(),
                exportTable.getColumns().size(),
                tcmContext.getTempDirectory()+tcmContext.getExportShellName(),
                tcmContext.getTempDirectory()+tcmContext.getCsvFileName()
        );
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> export data total time spent:{}",exportFromSource);

        logger.info("");

        start = System.currentTimeMillis();
        logger.info("*********************************** LOAD INFO ***********************************");
//====================== 8、create export data script,and execute export shell.
        tcm.loadTableData();
        end = System.currentTimeMillis();
        Long loadInClone = end-start;
        all+=loadInClone;
        logger.info("Load data situation:\nDatasource Type:{}\nTable Name:'{}'\nLoad Script Path:{}",
                cloneDataType,
                tcmContext.getCloneTable().getTableName(),
                tcmContext.getTempDirectory()+tcmContext.getLoadShellName());
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> load data total time spent:{}",loadInClone);
//        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Mapping Table and Syncing TableData table time:{} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",all);

        if(Boolean.TRUE.equals(deleteFlag)){
            tcm.deleteCache();
        }

        if(Boolean.TRUE.equals(debugFlag)){
            logger.info("\n\n\n\n");
            logger.info(tcmContext.toString());
        }

        String resultInfo = String.format(
                "\n" +
                        "%-20s |%15s\n" +
                        "%-20s |%15s ms \n" +
                        "%-20s |%15s ms \n" +
                        "%-20s |%15s ms \n" +
                        "%-20s |%15s ms \n" +
                        "%-20s |%15s ms \n"
                ,"Step","SpendTime",
                "GetSourceTable",getSourceTable,
                "CreateCloneTable",createCloneTable,
                "ExportFromSource",exportFromSource,
                "LoadInClone",loadInClone,
                "TotalTime",all);
        logger.info("\n{}\n",resultInfo);
        message.setCreateSourceTableTime(getSourceTable)
                .setCreateCloneTableTime(createCloneTable)
                .setExportFromSourceTime(exportFromSource)
                .setLoadInCloneTime(loadInClone)
                .setTotalTime(all);
        return message;
    }

}
