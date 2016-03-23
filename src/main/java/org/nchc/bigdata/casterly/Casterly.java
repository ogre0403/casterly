package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.filter.LogFileFilter;
import org.nchc.bigdata.filter.SparkLogFileFilter;
import org.nchc.bigdata.parser.SparkLogParserImpl;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.sql.SQLException;


public class Casterly {
    private static Logger logger = Logger.getLogger(Casterly.class);
    private static String[] FILTER_CLAZZ = {Const.FILTER_CLAZZ_SPARK, Const.FILTER_CLAZZ_MAPREDUCE};
    private static String[] PARSER_CLAZZ = {Const.PARSER_CLAZZ_SPARK, Const.PARSER_CLAZZ_MAPREDUCE};
    private static String[] DAO_CLAZZ = {Const.DAO_CLAZZ_SPARK, Const.DAO_CLAZZ_MAPREDUCE};

    public static void main( String[] args ) {

        Configuration conf = new Configuration();

/*
        LogMonitor sparkMtr = LogMonitor.createLogMonitor(
            conf, "   ",
            Const.FILTER_CLAZZ_SPARK,
            Const.PARSER_CLAZZ_SPARK,
            Const.DAO_CLAZZ_SPARK);
        if(sparkMtr != null) {
            sparkMtr.start();
            addShutdownHook(sparkMtr);
        }else
            logger.warn("Spark Monitor initialize fail!");
*/

        LogMonitor MRMtr = LogMonitor.createLogMonitor(
            conf, "/user/history/done/",
            Const.FILTER_CLAZZ_MAPREDUCE,
            Const.PARSER_CLAZZ_MAPREDUCE,
            Const.DAO_CLAZZ_MAPREDUCE
        );
        if(MRMtr != null) {
            MRMtr.start();
            addShutdownHook(MRMtr);
        }else
            logger.warn("MapReduce Monitor initialize fail!");


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
