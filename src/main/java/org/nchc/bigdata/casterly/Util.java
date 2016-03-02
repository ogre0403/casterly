package org.nchc.bigdata.casterly;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by 1403035 on 2016/3/2.
 */
public class Util {
    public static String traceString(Exception e){
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
