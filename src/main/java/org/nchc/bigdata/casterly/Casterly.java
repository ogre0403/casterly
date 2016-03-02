package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.filter.LogFileFilter;
import org.nchc.bigdata.filter.SparkLogFileFilter;
import org.nchc.bigdata.parser.SparkLogParserImpl;

import java.io.IOException;
import java.sql.SQLException;


public class Casterly {
    private static Logger logger = Logger.getLogger(Casterly.class);

    public static void main( String[] args ) throws IOException, SQLException {

        Configuration conf = new Configuration();

        // Setup Spark log file reader
        Path logPath = new Path(" path / to /spark / log");

        LogFileFilter sparkFileFilter = null;
        try {
            sparkFileFilter = new SparkLogFileFilter(conf);
        } catch (IOException e) {
            logger.error("Can not get HDFS");
            throw  e;
        } catch (SQLException e) {
            logger.error("Can not get DB");
            throw e;
        }
        Reader sparkReader = new Reader(conf, logPath);
        sparkReader.setFilter(sparkFileFilter);
        sparkReader.setParser(new SparkLogParserImpl());

        LogMonitor sparkMtr = new LogMonitor(
                sparkReader,      // Spark Log reader
                JoBDAOFactory.getJobDAO(Const.DAO_CLAZZ_SPARK, conf)    // SparkDAO
        );
        sparkMtr.run();
    }
}
