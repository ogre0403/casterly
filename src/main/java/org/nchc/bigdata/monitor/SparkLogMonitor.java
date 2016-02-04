package org.nchc.bigdata.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.nchc.bigdata.parser.Reader;
import org.nchc.bigdata.parser.SparkLogReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;


/**
 * Created by 1403035 on 2016/2/2.
 */
public class SparkLogMonitor extends Thread{

    private static int INTERVAL = 10000;

    private static Logger logger = Logger.getLogger(SparkLogMonitor.class);
    private boolean isRunning = true;
    private Reader sparkreader= null;

    public SparkLogMonitor(Configuration conf, Properties ps){
        /**
         * TODO: add DB info and path info
         **/
    }

    public SparkLogMonitor(Configuration conf) throws IOException, SQLException {
        sparkreader = new SparkLogReader(conf, new Path(""));
        /**
         * TODO: add DB info and path info
         **/
    }

    public void run(){
        while (isRunning) {
            try {

                sparkreader.readAllFile();


                Thread.sleep(INTERVAL);
            } catch (Exception e) {
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                logger.error(errors.toString());
            }
        }

    }
}
