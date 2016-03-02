package org.nchc.bigdata.casterly;

import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.JobModel;

import java.util.List;

import org.apache.log4j.Logger;


/**
 * Created by 1403035 on 2016/2/2.
 */
public class LogMonitor extends Thread{

    private static int INTERVAL = 10000;

    private static Logger logger = Logger.getLogger(LogMonitor.class);
    private boolean isRunning = true;
    private Reader reader = null;
    private JobDAO daoImpl = null;

    public LogMonitor(Reader reader, JobDAO impl)  {
        this.reader = reader;
        this.daoImpl = impl;
    }

    public void run(){
        while (isRunning) {
            try {
                List<JobModel> jobs = reader.readAllFile();
                daoImpl.add(jobs);
                Thread.sleep(INTERVAL);
            } catch (Exception e) {
                logger.error(Util.traceString(e));
            }
        }
    }
}
