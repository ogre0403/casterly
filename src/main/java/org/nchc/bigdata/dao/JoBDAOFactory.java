package org.nchc.bigdata.dao;


import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

/**
 * Created by 1403035 on 2016/2/18.
 */
public class JoBDAOFactory {

    private static Logger logger = Logger.getLogger(JoBDAOFactory.class);

    private static SparkJobDAOImpl dao = null;
    private static HashMap<String, JobDAO> daos = new HashMap<>();


    public static JobDAO getJobDAO(String clazz, Configuration conf){

        JobDAO dd = daos.get(clazz);
        if (dd != null) {
            return dd;
        }
        JobDAO result = null;
        try {
             result = (JobDAO) Class.forName(clazz)
                    .getDeclaredConstructor(Configuration.class).newInstance(conf);

        } catch (InstantiationException e) {
            logger.error(Util.traceString(e));
        } catch (IllegalAccessException e) {
            logger.error(Util.traceString(e));
        } catch (InvocationTargetException e) {
            logger.error(Util.traceString(e));
        } catch (NoSuchMethodException e) {
            logger.error(Util.traceString(e));
        } catch (ClassNotFoundException e) {
            logger.error(Util.traceString(e));
        }

        daos.put(clazz,result);
        return result;
    }

    public static SparkJobDAOImpl getSparkJobDAO(Configuration conf){
        if ( dao != null ) {
            return dao;
        }

        try {
            dao = ( SparkJobDAOImpl )Class.forName(Const.DAO_CLAZZ_SPARK)
                    .getDeclaredConstructor(Configuration.class).newInstance(conf);
        } catch (InstantiationException e) {
            logger.error(Util.traceString(e));
        } catch (IllegalAccessException e) {
            logger.error(Util.traceString(e));
        } catch (InvocationTargetException e) {
            logger.error(Util.traceString(e));
        } catch (NoSuchMethodException e) {
            logger.error(Util.traceString(e));
        } catch (ClassNotFoundException e) {
            logger.error(Util.traceString(e));
        }
        return dao;
    }

}
