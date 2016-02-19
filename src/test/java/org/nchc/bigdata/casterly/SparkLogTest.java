package org.nchc.bigdata.casterly;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nchc.bigdata.model.SparkJobModel;
import org.nchc.bigdata.parser.*;
import org.nchc.bigdata.parser.Reader;

import java.io.*;
import java.sql.SQLException;


public class SparkLogTest {
//    private static Logger logger = Logger.getLogger(SparkLogTest.class);

    private static Log logger = LogFactory.getLog(SparkLogTest.class);

    private static String DEFAULTEVENTDIR = "/user/spark/applicationHistory/";
    private static String FAILLOG = "application_1452487986830_0003";
    private static String NOATTEMPTID = "application_1452487986830_0003";
    private static String SUCCESSLOG = "application_1452487986830_0002_1";
    private static String ATTEMPTID = "application_1452487986830_0002_1";
    private static String INPROGRESS = "application_1452819854282_0011.inprogress";

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        // Start mini cluster

    }

    @Test
    public void testFailEventLogReader() throws IOException, SQLException {
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/spark/" + FAILLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader(new Configuration());
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);
        logger.info(r.getAppStart().getId());
        logger.info(r.getExecutorAdd().size());

    }

    @Test
    public void testSuccessEventLogReader() throws IOException, SQLException {
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/spark/" + SUCCESSLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader(new Configuration());
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);
        logger.info(r.getAppStart().getId());
        logger.info(r.getExecutorAdd().size());
    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }
}
