package com.boraydata.tcm;

import com.boraydata.tcm.configuration.TableCloneManageConfig;
import com.boraydata.tcm.core.TableCloneManage;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.core.TableCloneManageFactory;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.exception.TCMException;
import com.boraydata.tcm.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** TCM - Table Clone Manage and Table Data Sync
 * @author bufan
 * @data 2021/9/28
 */
public class DoIt {

    public static void main(String[] args)  {

        Logger logger = LoggerFactory.getLogger(DoIt.class);
        Boolean deleteFlag;
        Boolean debugFlag;
        String sourceTableName;
        String cloneTableName;
        String sourceDataType;
        String cloneDataType;

        Properties properties;
        TableCloneManageConfig config;
        long start = 0;
        long end = 0;
        long all = 0;

        String configFilePath;
        if(args == null || args.length <= 0)
//            configFilePath = "./test.properties";
            throw new TCMException("you should incoming properties File.");
        else
            configFilePath = args[0];

//        if(!new File(configFilePath).exists())
//            throw new TCMException("the '"+configFilePath+"' properties File is not found.");
        try(FileInputStream in = new FileInputStream(configFilePath)){
//        try(InputStream in = DoIt.class.getClassLoader().getResourceAsStream("./test.properties")){
            properties = new Properties();
            properties.load(in);
            config = TableCloneManageConfig.getInstance();
//====================== 1. read the properties file and initialize the information.
            config.loadLocalConfig(properties);

            deleteFlag = config.getDeleteCache();
            debugFlag = config.getDebug();
            sourceTableName = config.getSourceConfig().getTableName();
            cloneTableName = config.getCloneConfig().getTableName();
            sourceDataType = config.getSourceConfig().getDataSourceType().toString();
            cloneDataType = config.getCloneConfig().getDataSourceType().toString();
        } catch (IOException e) {
            throw new TCMException("the '"+configFilePath+"' is not found",e);
        }
        if(Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Source Config Info ==================================\n" +
                    "{}" +
                        "\n================================== Clone Config Info ==================================\n" +
                    "{}\n",config.getSourceConfig().getCofInfo(),config.getCloneConfig().getCofInfo());

//====================== 2. Create TableCloneManageContext, pass the read information to context
        TableCloneManageContext tcmContext = new TableCloneManageContext.Builder()
                .setTcmConfig(config)
                .create();

//====================== 3、 use Context create TCM
        TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);



                                    start = System.currentTimeMillis();
        logger.info("Ready to get Source Table structure and information:{}->{}",sourceDataType,sourceTableName);
//====================== 4、 get Source Table Struct by tableName in sourceData
        Table sourceTable = tcm.createSourceMappingTable(sourceTableName);
                                    end = System.currentTimeMillis();
                                    Long getSourceTable = end-start;
                                    all+=(end - start);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> get Source Table info total time spent:{}",getSourceTable);
        if (Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Source Table Info ==================================\n" +
                    "{}",sourceTable.getTableInfo());




                                    start = System.currentTimeMillis();
        logger.info("Ready to create Clone Table '{}' in {}",cloneTableName,cloneDataType);
//====================== 5、 get Clone Table based on Source Table
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneTableName);
//====================== 6、create Clone Table in clone database
        tcm.createTableInDatasource();
                                    end = System.currentTimeMillis();
                                    Long createCloneTable = end-start;
                                    all+=(end - start);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> create Clone Table total time spent:{}",createCloneTable);
         if (Boolean.TRUE.equals(debugFlag))
             logger.info("\n================================== Clone Table Info ==================================\n" +
                     "{}",cloneTable.getTableInfo());



                                    start = System.currentTimeMillis();
        logger.info("*********************************** EXPORT INFO ***********************************");
                ;
//====================== 7、create export data script,and execute export shell.
        tcm.exportTableData();
                                    end = System.currentTimeMillis();
                                    Long exportFromSource = end-start;
            all+=(end-start);
        logger.info("\nexport '{}:{}'\ncsvPath:{}\nscript:{}",sourceDataType,tcmContext.getFinallySourceTable().getTableName()
                ,tcmContext.getTempDirectory()+tcmContext.getCsvFileName(),tcmContext.getTempDirectory()+tcmContext.getExportShellName());
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> export data total time spent:{}",exportFromSource);

        logger.info("");

                                    start = System.currentTimeMillis();
        logger.info("*********************************** LOAD INFO ***********************************");
//====================== 8、create export data script,and execute export shell.
        tcm.loadTableData();
                                    end = System.currentTimeMillis();
                                    Long loadInClone = end-start;
            all+=(end-start);
        logger.info("\nload data in '{}:{}'\nscript:{}",cloneDataType,cloneTableName,tcmContext.getTempDirectory()+tcmContext.getLoadShellName());
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> load data total time spent:{}",loadInClone);
//        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Mapping Table and Syncing TableData table time:{} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",all);

        if(Boolean.TRUE.equals(deleteFlag)){
            tcm.deleteCache();
        }

        if(Boolean.TRUE.equals(debugFlag)){
//            String exportShellContent = tcmContext.getExportShellContent();
//            String loadDataScriptContent = tcmContext.getLoadDataInHudiScalaScriptContent();
//            String loadShellContent = StringUtil.isNullOrEmpty(tcmContext.getLoadShellContent())?"No script file is generated,maybe load data by JDBC. ":tcmContext.getLoadShellContent();
            logger.info("\n\n\n\n");
            logger.info(tcmContext.toString());
//            logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getExportShellName(),exportShellContent);
//            if(!StringUtil.isNullOrEmpty(tcmContext.getLoadDataInHudiScalaScriptName()))
//                logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getLoadDataInHudiScalaScriptName(),loadDataScriptContent);
//            logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getLoadShellName(),loadShellContent);
//            logger.info("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
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
    }

}
