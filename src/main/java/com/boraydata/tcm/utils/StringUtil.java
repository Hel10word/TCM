package com.boraydata.tcm.utils;

import com.boraydata.tcm.core.TableCloneManageType;

import java.util.Locale;
import java.util.Map;
import java.util.Random;

public class StringUtil {
    // judgment String is null or empty
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    // Format DataType
    public static String dataTypeFormat(String string){
        // e.g. : var char  -->  varchar
        string = string.replaceAll(" ","");
//        if(string.equalsIgnoreCase("bit(1)"))
//            return string;
        if(string.equalsIgnoreCase("tinyint(1)"))
            return string;
        // e.g. :  bit(100) -->  bit(>1)
//        else if (string.toLowerCase(Locale.US).replaceAll("bit\\(\\d*\\)","").length()==0)
//            return string.replaceAll("\\d+",">1");

        // e.g. : varchar(16)  -->   varchar
        return string.replaceAll("\\(.*\\)","");
    }

    public static String getRandom(){
        Random random = new Random();
        int nextInt = random.nextInt(9000000);
        nextInt += 1000000;
        return String.valueOf(nextInt);
    }

//    public static boolean equalsIgnoreCase(String A,String B){
//        return A.equalsIgnoreCase(B);
//    }

    /**
     * @Param map : database datatype mapping relationship Map.          e.g. : {("int":INT),("integer":INT)}
     * @Param string : Need to be mapped to the TCM datatype.            e.g. : int , integer
     * @Param def : if not found relation in define map,can set default.
     * @Return: TableCloneManageType : find a type in TableCloneManageType define. e.g. : INT , INT
     */
    public static TableCloneManageType findRelation(Map<String, TableCloneManageType> map, String string, TableCloneManageType def){
        return map.keySet().stream().filter(
                x -> x.equalsIgnoreCase(StringUtil.dataTypeFormat(string))
        ).findFirst().map(x -> map.get(x)).orElse(def);
    }
}
