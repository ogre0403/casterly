package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.filter.LogFileFilter;
import org.nchc.bigdata.model.JobModel;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.log4j.Logger;
import org.nchc.bigdata.parser.IParser;


/**
 * Created by 1403035 on 2016/2/2.
 */
public class LogMonitor extends Thread{

    private int INTERVAL = 10000;

    private static Logger logger = Logger.getLogger(LogMonitor.class);
    private boolean isRunning = true;
    private Reader reader = null;
    private JobDAO daoImpl = null;

    public LogMonitor(Reader reader, JobDAO impl)  {
        this.reader = reader;
        this.daoImpl = impl;
    }

    public void setInterval(int interval){
        this.INTERVAL = interval;
    }

    public void run(){
        while (isRunning) {
            try {
                List<JobModel> jobs = reader.readAllFile(true);
                logger.debug("Size =  " + jobs.size());
                daoImpl.add(jobs);
                Thread.sleep(INTERVAL);
            }catch(InterruptedException e) {
                logger.info("Thread was inturrupted");
            }
            catch (Exception e) {
                logger.error(Util.traceString(e));
            }
        }
    }

    public void stopThread(){
        logger.info("stop LogMonitor");
        reader.close();
        daoImpl.close();
        this.isRunning = false;
    }

    public static LogMonitor createLogMonitor(
            Configuration conf, String path,
            String FilterClazz,
            String ParserClazz,
            String DAOClazz,
            int interval){
        Path logPath = new Path(path);

        LogFileFilter fileFilter = null;
        IParser fileParser = null;
        try {
            fileFilter = (LogFileFilter) Class.forName(FilterClazz)
                    .getDeclaredConstructor(Configuration.class).newInstance(conf);
        } catch (InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException | IllegalAccessException |
                 InstantiationException  e ) {
            logger.error(Util.traceString(e));
        }

        try{
            fileParser  = (IParser) Class.forName(ParserClazz).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException |
                 InstantiationException e) {
            logger.error(Util.traceString(e));
        }

        if (fileFilter == null || fileParser == null){
            return null;
        }
        Reader reader = new Reader(conf, logPath);
        reader.setFilter(fileFilter);
        reader.setParser(fileParser);
        LogMonitor monitor = new LogMonitor(
                reader,      // Log reader
                JoBDAOFactory.getJobDAO(DAOClazz, conf)    // DAO
        );
        monitor.setInterval(interval);
        return monitor;
    }
}
