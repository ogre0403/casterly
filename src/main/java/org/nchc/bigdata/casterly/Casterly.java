package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import java.util.Properties;


public class Casterly {
    private static Logger logger = Logger.getLogger(Casterly.class);

    private static String[] FILTER_CLAZZ =
            {   Const.FILTER_CLAZZ_MAPREDUCE,
                Const.FILTER_CLAZZ_SPARK };

    private static String[] PARSER_CLAZZ =
            {   Const.PARSER_CLAZZ_MAPREDUCE,
                Const.PARSER_CLAZZ_SPARK };

    private static String[] DAO_CLAZZ =
            {   Const.DAO_CLAZZ_MAPREDUCE,
                Const.DAO_CLAZZ_SPARK   };

    private static String[] MONITOR_PATH;

    public static void main( String[] args ) {

        Configuration conf = new Configuration();
        Properties properties = Util.loadProperties(Const.PROPFILENAME);

        if(properties == null){
            logger.error(Const.PROPFILENAME + " not exist!!");
            return;
        }

        if(properties.containsKey(Const.SQL_URL) &&
                properties.containsKey(Const.SQL_USER) &&
                properties.containsKey(Const.SQL_PASSWORD)) {
            conf.set(Const.SQL_URL, properties.getProperty(Const.SQL_URL));
            conf.set(Const.SQL_USER, properties.getProperty(Const.SQL_USER));
            conf.set(Const.SQL_PASSWORD, properties.getProperty(Const.SQL_PASSWORD));
        }else{
            logger.error("SQL setting is not configured!!");
            return;
        }

        int interval = 1000;
        if(properties.containsKey(Const.MONITOR_INTERVAL_SEC)){
            interval = Integer.parseInt(
                    properties.getProperty(Const.MONITOR_INTERVAL_SEC, "10000"));
        }

        MONITOR_PATH = new String[] {
                properties.getProperty("MAPREDUCE_LOG_PATH","/user/history/done/"),
                properties.getProperty("SPARK_LOG_PATH","/user/spark/applicationHistory")
        };

        LogMonitor monitor = null;
        for(int i = 0; i < 2 ; i++){
            monitor = LogMonitor.createLogMonitor(conf, MONITOR_PATH[i],
                    FILTER_CLAZZ[i], PARSER_CLAZZ[i],DAO_CLAZZ[i],interval
                    );
            if(monitor != null) {
                monitor.start();
                addShutdownHook(monitor);
            }else {
                logger.warn(FILTER_CLAZZ[i] +  " initialize fail!");
                logger.warn(PARSER_CLAZZ[i] +  " initialize fail!");
                logger.warn(DAO_CLAZZ[i] +  " initialize fail!");
            }
        }
    }

    private static void addShutdownHook(final LogMonitor thread){
        Runtime.getRuntime().addShutdownHook(
            new Thread(){
                public void run(){
                    thread.stopThread();
                    thread.interrupt();
                }
            }
        );
    }
}
