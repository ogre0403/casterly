package org.nchc.bigdata.dao;


import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by 1403035 on 2016/2/18.
 */
public class JoBDAOFactory {

    private static Logger logger = Logger.getLogger(JoBDAOFactory.class);

    private static final String SPARKJOBDAOIMPL = "org.nchc.bigdata.dao.SparkJobDAOImpl";
    private static SparkJobDAOImpl dao = null;

    public static SparkJobDAOImpl getSparkJobDAO() {
        // If we already have loaded the DAO, return it
        if ( dao != null ) {
            return dao;
        }

        try {
            dao = ( SparkJobDAOImpl )Class.forName(SPARKJOBDAOIMPL).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return dao;
    }


    public static SparkJobDAOImpl getSparkJobDAO(Configuration conf){
        if ( dao != null ) {
            return dao;
        }

        try {
            dao = SparkJobDAOImpl.class.getDeclaredConstructor(Configuration.class).newInstance(conf);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return dao;
    }

}
