package org.nchc.bigdata.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.parser.Reader;
import org.nchc.bigdata.parser.SparkFileFilter;
import org.nchc.bigdata.parser.SparkLogParserImpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * Created by 1403035 on 2016/2/2.
 */
public class SparkLogMonitor extends Thread{

    private static int INTERVAL = 10000;

    private static Logger logger = Logger.getLogger(SparkLogMonitor.class);
    private boolean isRunning = true;
    private Reader sparkReader = null;
    private JobDAO sparkJobDAO = null;

    public SparkLogMonitor(Configuration conf) throws IOException, SQLException {
        /**
         * TODO: add DB info and path info
         **/
        Path logPath = new Path(conf.get(""));
        sparkReader = new Reader(conf, logPath);
        sparkReader.setFilter(new SparkFileFilter(conf));
        sparkReader.setParser(new SparkLogParserImpl());
        this.sparkJobDAO = JoBDAOFactory.getSparkJobDAO(conf);
    }

    public void run(){
        while (isRunning) {
            try {
                List<JobModel> jobs = sparkReader.readAllFile();
                sparkJobDAO.add(jobs);
                Thread.sleep(INTERVAL);
            } catch (Exception e) {
                StringWriter errors = new StringWriter();
                e.printStackTrace(new PrintWriter(errors));
                logger.error(errors.toString());
            }
        }
    }
}
