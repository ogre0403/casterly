package org.nchc.bigdata.casterly;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

/**
 * Created by 1403035 on 2016/3/2.
 */
public class Util {
    public static String traceString(Exception e){
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    public static Properties loadProperties(String propertiesFile) {
        Properties prop = new Properties();
        InputStream inp = Util.class.getClassLoader().getResourceAsStream(propertiesFile);

        try {
            if (inp == null){
                return null;
            }
            prop.load(inp);
            return prop;
        }catch (IOException e) {
            return null;
        } finally {
            if (inp != null) {
                try {
                    inp.close();
                } catch (IOException ignore) {
                    // do nothing
                }
            }
        }
    }

}
