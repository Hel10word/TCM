package com.boraydata.tcm.utils;

import com.boraydata.tcm.core.DataTypeMapping;

import java.util.Locale;
import java.util.Map;

public class StringUtil {
    // judgment String is null or empty
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    // Format DataType
    public static String dataTypeFormat(String string){
        // e.g. : var char  -->  varchar
        string = string.replaceAll(" ","");
        if(string.equalsIgnoreCase("bit(1)"))
            return string;
        // e.g. :  bit(100) -->  bit(>1)
        else if (string.toLowerCase(Locale.US).replaceAll("bit\\(\\d*\\)","").length()==0)
            return string.replaceAll("\\d+",">1");

        // e.g. : varchar(16)  -->   varchar
        return string.replaceAll("\\([\\d\\,]*\\)","");
    }

//    public static boolean equalsIgnoreCase(String A,String B){
//        return A.equalsIgnoreCase(B);
//    }

    /**
     * @Param map : database datatype mapping relationship Map.          e.g. : {("int":INT),("integer":INT)}
     * @Param string : Need to be mapped to the TCM datatype.            e.g. : int , integer
     * @Param def : if not found relation in define map,can set default.
     * @Return: DataTypeMapping : find a type in DataTypeMapping define. e.g. : INT , INT
     */
    public static DataTypeMapping findRelation(Map<String,DataTypeMapping> map,String string,DataTypeMapping def){
        return map.keySet().stream().filter(
                x -> x.equalsIgnoreCase(StringUtil.dataTypeFormat(string))
        ).findFirst().map(x -> map.get(x)).orElse(def);
    }
}
