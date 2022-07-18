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
        Boolean useCustomTables;
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
        } catch (IOException e) {
            throw new TCMException("make sure the config file exist",e);
        }
        try {
            config = config.checkConfig();
            deleteFlag = config.getDeleteCache();
            debugFlag = config.getDebug();
            useCustomTables = config.getUseCustomTables();
            sourceTableName = config.getSourceTableName();
            cloneTableName = config.getCloneTableName();
            sourceDataType = config.getSourceConfig().getDataSourceEnum().toString();
            cloneDataType = config.getCloneConfig().getDataSourceEnum().toString();
        }catch (Exception e){
            Message message = new Message().setCode(400).setMessage("Please make sure the configuration file is filled correctly \n"+e.getMessage());
            RedisOperateUtil.sendMessage(config.getRedis(),JacksonUtil.toJson(message));
            throw new TCMException("Please make sure the configuration file is filled correctly",e);
        }
        LinkedList<String> sourceTableNames = config.getSourceTableNames();
        LinkedList<String> cloneTableNames = config.getCloneTableNames();
        LinkedList<Table> customTables = config.getCustomTables();
        LinkedList<Message> messageList = new LinkedList<>();
        if(null != cloneTableNames) {
            for (int i = 0; i < cloneTableNames.size(); i++) {
                cloneTableName = cloneTableNames.get(i);
                if(Boolean.TRUE.equals(useCustomTables)){
                    if(customTables != null && !customTables.isEmpty() && i <= customTables.size()){
                        Table customTable = customTables.get(i);
                        sourceTableName = customTable.getTableName();
                        config.setCustomTable(customTable);
                    }else {
                        messageList.add(new Message().setCode(400).setMessage("The number of 'useCustomTables' and 'cloneTableNames' are inconsistent, please re-fill configuration file."));
                        continue;
                    }
                }else {
                    if(i <= sourceTableNames.size()) {
                        sourceTableName = sourceTableNames.get(i);
                    }else {
                        messageList.add(new Message().setCode(400).setMessage("The number of 'sourceTableName' and 'cloneTableNames' are inconsistent, please re-fill configuration file."));
                        continue;
                    }
                }
                config.setSourceTableName(sourceTableName);
                config.setCloneTableName(cloneTableName);
                Message message = tableCloneManager(config, deleteFlag, debugFlag, sourceTableName, cloneTableName, sourceDataType, cloneDataType);
                messageList.add(message);
            }
        }else {
            if(Boolean.FALSE.equals(useCustomTables))
                config.setCustomTable(null);
            Message message = tableCloneManager(config, deleteFlag, debugFlag, sourceTableName, cloneTableName, sourceDataType, cloneDataType);
            messageList.add(message);
        }
        if(Boolean.TRUE.equals(config.getSendRedis())) {
            logger.info("send result to Redis,key:{}",config.getRedis().getMessageKey());
            if (messageList.size() == 1)
                RedisOperateUtil.sendMessage(config.getRedis(), JacksonUtil.toJson(messageList.get(0)));
            else
                RedisOperateUtil.sendMessage(config.getRedis(), JacksonUtil.toJson(messageList));
        }

    }

    private static Message tableCloneManager(TableCloneManagerConfig config, Boolean deleteFlag, Boolean debugFlag, String sourceTableName, String cloneTableName, String sourceDataType, String cloneDataType){
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

//        StringBuilder sb = new StringBuilder()
//                .append("Source Table : ").append(config.getSourceTableName()).append("\n")
//                .append("Source Data : ").append(JacksonUtil.toJson(config.getSourceConfig())).append("\n")
//                .append("Clone Table : ").append(config.getCloneTableName()).append("\n")
//                .append("Clone Data : ").append(JacksonUtil.toJson(config.getCloneConfig())).append("\n")
//                ;
//        message.setJobInfo(sb.toString());


        start = System.currentTimeMillis();
        logger.info("Ready to get Source Table metadata information,Table Name:'{}'  Datasource Type:{}",sourceTableName,sourceDataType);
//====================== 4、 get Source Table Struct by tableName in sourceData
        Table sourceTable;
        try {
            sourceTable = tcm.createSourceMappingTable(sourceTableName);
        }catch (Exception e){
            return message.setMessage("failed to createSourceMappingTable,sourceTableName:"+sourceTableName+"\n"+e.getMessage());
        }
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
        Table cloneTable;
        try {
            cloneTable= tcm.createCloneTable(sourceTable,cloneTableName);
        }catch (Exception e){
            return message.setMessage("failed to createCloneTable,cloneTableName:"+cloneTableName+"\n"+e.getMessage());
        }

//====================== 6、create Clone Table in clone database
        try {
            tcm.createTableInDatasource();
        }catch (Exception e){
            return message.setMessage("failed to createTableInDatasource,\n"+e.getMessage());
        }
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
        try {
            tcm.exportTableData();
        }catch (Exception e){
            return message.setMessage("failed to exportTableData,\n"+e.getMessage());
        }
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
        try {
            tcm.loadTableData();
        }catch (Exception e){
            return message.setMessage("failed to loadTableData,\n"+e.getMessage());
        }
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
            try {
                tcm.deleteCache();
            }catch (Exception e){
                return message.setMessage("failed to deleteCache,\n"+e.getMessage());
            }
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
