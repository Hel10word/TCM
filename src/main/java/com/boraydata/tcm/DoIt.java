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
import java.util.Properties;

/** TCM - Table Clone Manage and Table Data Sync
 * @author bufan
 * @data 2021/9/28
 */
public class DoIt {

    public static void main(String[] args)  {

        Logger logger = LoggerFactory.getLogger(DoIt.class);
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
//            configFilePath = "./config.properties";
            throw new TCMException("you should incoming properties File.");
        else
            configFilePath = args[0];

        if(!new File(configFilePath).exists())
            throw new TCMException("the '"+configFilePath+"' properties File is not found.");
        try(FileInputStream in = new FileInputStream(configFilePath)){
//        try(InputStream in = DoIt.class.getClassLoader().getResourceAsStream("./config.properties")){
            properties = new Properties();
            properties.load(in);
            config = TableCloneManageConfig.getInstance();
//====================== 1. read the properties file and initialize the information.
            config.loadLocalConfig(properties);

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
                                    all+=(end - start);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> get Source Table info total time spent:{}\n",(end - start));
        if (Boolean.TRUE.equals(debugFlag))
            logger.info("\n================================== Source Table Info ==================================\n" +
                    "{}\n",sourceTable.getTableInfo());




                                    start = System.currentTimeMillis();
        logger.info("Ready to create Clone Table '{}' in {}",cloneTableName,cloneDataType);
//====================== 5、 get Clone Table based on Source Table
        Table cloneTable = tcm.createCloneTable(sourceTable,cloneTableName);
//====================== 6、create Clone Table in clone database
        tcm.createTableInDatasource();
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> create Clone Table total time spent:{}\n",(end - start));
         if (Boolean.TRUE.equals(debugFlag))
             logger.info("\n================================== Clone Table Info ==================================\n" +
                     "{}\n",cloneTable.getTableInfo());



                                    start = System.currentTimeMillis();
        logger.info("Read to Export Source Table data in CSV.");
//====================== 7、create export data script,and execute export shell.
        tcm.exportTableData();
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
        logger.info("*********************************** EXPORT INFO ***********************************\n\nexport {}:{} in {} script:'{}'\n",sourceDataType,tcmContext.getFinallySourceTable().getTableName(),
                tcmContext.getTempDirectory()+tcmContext.getCsvFileName(),tcmContext.getTempDirectory()+tcmContext.getExportShellName());
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> export data total time spent:{}\n\n",(end - start));


                                    start = System.currentTimeMillis();
        logger.info("Read to Load CSV data in Clone Table.");
//====================== 8、create export data script,and execute export shell.
        tcm.loadTableData();
                                    end = System.currentTimeMillis();
                                    all+=(end - start);
        logger.info("*********************************** LOAD INFO ***********************************\n\nload data in {}:{} script:'{}'\n",cloneDataType,cloneTableName,tcmContext.getTempDirectory()+tcmContext.getLoadShellName());
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> load data total time spent:{}",(end - start));
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Mapping Table and Syncing TableData table time:{} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",all);





        if(Boolean.TRUE.equals(debugFlag)){
            String exportShellContent = tcmContext.getExportShellContent();
            String loadDataScriptContent = tcmContext.getLoadDataScriptContent();
            String loadShellContent = tcmContext.getLoadShellContent();
            logger.info("\n\n\n\n");
            logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getExportShellName(),exportShellContent);
            if(!StringUtil.isNullOrEmpty(tcmContext.getLoadDataScriptName()))
                logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getLoadDataScriptName(),loadDataScriptContent);
            logger.info("------------------------------------------------------------------------------------- {} -------------------------------------------------------------------------------------\n\n{}\n",tcmContext.getLoadShellName(),loadShellContent);
            logger.info("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        }
    }

}
