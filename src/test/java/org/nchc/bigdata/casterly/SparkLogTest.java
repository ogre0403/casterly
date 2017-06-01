package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Logger;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.nchc.bigdata.filter.SparkLogFileFilter;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.SparkJobModel;
import org.nchc.bigdata.parser.*;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SparkLogTest {
    private static Logger logger = Logger.getLogger(SparkLogTest.class);

    private static String DEFAULTEVENTDIR = "/user/spark/applicationHistory/";
    private static String FAILLOG = "application_1452487986830_0003";
    private static String SUCCESSLOG = "application_1452487986830_0002_1";
    private static String INPROGRESS = "application_1452819854282_0011.inprogress";
    private static String SUCCESSLOG2 = "application_1452819854282_0001";
    private static String tsengLog = "application_1491786134915_10290";



    private static final String CLUSTER_1 = "cluster1";
    private static IDatabaseTester databaseTester;

    private  static File testDataPath;
    private  static Configuration conf;
    private  static MiniDFSCluster cluster;
    private  static FileSystem fs;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        // Start mini cluster
        /*
        testDataPath = new File(
                PathUtils.getTestDir(SparkLogTest.class.getClass()),
                "miniclusters");
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();
        File testDataCluster1 = new File(testDataPath, CLUSTER_1);
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = FileSystem.get(conf);
*/
        // start database
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
        createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());

        // set db connection info into configuration
        conf.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        conf.set(Const.SQL_USER, "sa");
        conf.set(Const.SQL_PASSWORD, "");
    }

    private static void createTablesSinceDbUnitDoesNot(Connection connection)
            throws SQLException {

        PreparedStatement last_processed =
                connection.prepareStatement(
                "CREATE  TABLE  LAST_PROCESSED (" +
                        "ID INT, LAST BIGINT)");
        last_processed.execute();
        last_processed = connection.prepareStatement("INSERT INTO LAST_PROCESSED (ID, LAST) VALUES (1,100)");
        last_processed.execute();
        last_processed.close();
    }

    @Test
    public void testFailEventLogReader() throws IOException, SQLException {
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/"+tsengLog);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader();
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);
        logger.info(r.getAppStart().getId());
        logger.info(r.getExecutorAdd().size());
        logger.info(r.getExecutorRemoved().size());

        Map<String, SparkJobModel.ExecutorRemoved> map = r.getExecutorRemoved();

        Iterator<Map.Entry<String, SparkJobModel.ExecutorRemoved>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, SparkJobModel.ExecutorRemoved> entry = iter.next();

            logger.info(entry.getValue().getTime());
        }

    }

    @Ignore
    public void testSuccessEventLogReader() throws IOException, SQLException {
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream( "/" + SUCCESSLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader();
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);
        logger.info(r.getAppStart().getId());
        logger.info(r.getExecutorAdd().size());
    }

    private static long temp =0L;
    @Ignore
    public void testFilter1() throws IOException, SQLException {
        testUtil.putToHDFS("/" + SUCCESSLOG, DEFAULTEVENTDIR, fs, conf);
        testUtil.putToHDFS("/" + INPROGRESS, DEFAULTEVENTDIR, fs, conf);
        SparkLogFileFilter sparkFileFilter = new SparkLogFileFilter(conf);
        Reader sparkReader = new Reader(conf, new Path(DEFAULTEVENTDIR));
        sparkReader.setFilter(sparkFileFilter);
        sparkReader.setParser(new SparkLogParserImpl());
        List<JobModel> e = sparkReader.readAllFile(true);
        temp = sparkFileFilter.getLastProcessedFileModifiedTime();
        Assert.assertEquals("application_1452487986830_0002", ((SparkJobModel) e.get(0)).getAppStart().getId());
        Assert.assertEquals(e.size(), 1);
    }

    @Ignore
    public void testFilter2() throws IOException, SQLException, InterruptedException {
        logger.info(temp);

        logger.info("Generate log file atfer 5 sec.");
        Thread.sleep(5000);
        testUtil.putToHDFS("/" + SUCCESSLOG2, DEFAULTEVENTDIR, fs, conf);
        Path p = new Path(DEFAULTEVENTDIR + SUCCESSLOG2);
        FileStatus fileStatus = fs.getFileStatus(p);
        logger.info(fileStatus.getModificationTime());

        logger.info("Generate another log file atfer 5 sec.");
        Thread.sleep(5000);
        testUtil.putToHDFS("/" + FAILLOG, DEFAULTEVENTDIR, fs, conf);
        Path p2 = new Path(DEFAULTEVENTDIR + FAILLOG);
        FileStatus fileStatus2 = fs.getFileStatus(p2);
        logger.info(fileStatus2.getModificationTime());

        SparkLogFileFilter sparkFileFilter = new SparkLogFileFilter(conf);
        Assert.assertEquals(temp, sparkFileFilter.getLastProcessedFileModifiedTimeFromDB());

        Reader sparkReader = new Reader(conf, new Path(DEFAULTEVENTDIR));
        sparkReader.setFilter(sparkFileFilter);
        sparkReader.setParser(new SparkLogParserImpl());
        List<JobModel> e = sparkReader.readAllFile(true);
        Assert.assertEquals(e.size(), 2);

        for(JobModel model : e){
            logger.info(((SparkJobModel) model).getAppStart().getId());
        }
    }



//    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Path dataDir = new Path(
                testDataPath.getParentFile().getParentFile().getParent());
        fs.delete(dataDir, true);
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
        String rootTestDir = rootTestFile.getAbsolutePath();
        Path rootTestPath = new Path(rootTestDir);
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
        localFileSystem.delete(rootTestPath, true);
        cluster.shutdown();

        databaseTester.onTearDown();
    }
}
