package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.parser.Reader;
import org.nchc.bigdata.parser.SparkFileFilter;
import org.nchc.bigdata.parser.SparkLogParserImpl;


public class Casterly {
    public static void main( String[] args ) {

        Configuration conf = new Configuration();

        // Setup Spark log file reader
        Path logPath = new Path(" path / to /spark / log");
        Reader sparkReader = new Reader(conf, logPath);
        sparkReader.setFilter(new SparkFileFilter(conf));
        sparkReader.setParser(new SparkLogParserImpl());

        LogMonitor sparkMtr = new LogMonitor(
                sparkReader,      // Spark Log reader
                JoBDAOFactory.getJobDAO(Const.DAO_CLAZZ_SPARK, conf)    // SparkDAO
        );
        sparkMtr.run();
    }
}
