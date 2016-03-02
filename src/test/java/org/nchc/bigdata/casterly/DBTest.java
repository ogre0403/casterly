package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.junit.*;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;
import org.nchc.bigdata.filter.SparkLogFileFilter;
import org.nchc.bigdata.parser.SparkLogParserImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class DBTest {
    private static Logger logger = Logger.getLogger(DBTest.class);
    private static String SUCCESSLOG = "application_1452487986830_0002_1";
    private static IDatabaseTester databaseTester;


    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
//                "jdbc:hsqldb:file:/opt/dao/testdb", "sa", "");
        createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());


    }

    private static void createTablesSinceDbUnitDoesNot(Connection connection)
            throws SQLException {
        PreparedStatement app_sum = connection.prepareStatement(
                "CREATE TABLE APP_SUMMARY (" +
                        "EPOCH BIGINT, SEQ SMALLINT, " +
                        "USER CHAR(16), JOBNAME CHAR(255), QUEUE CHAR(32), " +
                        "START BIGINT , FINISH BIGINT, CPUHOUR BIGINT)");
        app_sum.execute();
        app_sum.close();

        PreparedStatement task_detail = connection.prepareStatement(
                "CREATE TABLE TASK_DETAIL (" +
                        "EPOCH BIGINT, SEQ SMALLINT,  TYPE CHAR(1), " +
                        "ID INT, START BIGINT, FINISH BIGINT) ");
        task_detail.execute();
        task_detail.close();

        PreparedStatement executor_detail = connection.prepareStatement(
                "CREATE TABLE EXECUTOR_DETAIL (" +
                "EPOCH BIGINT, SEQ SMALLINT, ID SMALLINT, START BIGINT)");
        executor_detail.execute();
        executor_detail.close();

        PreparedStatement last_processed = connection.prepareStatement(
                "CREATE  TABLE  LAST_PROCESSED (" +
                        "ID INT, LAST BIGINT)");
        last_processed.execute();
        last_processed = connection.prepareStatement("INSERT INTO LAST_PROCESSED (ID, LAST) VALUES (1,100)");
        last_processed.execute();
        last_processed.close();
    }

    @Test
    public void testFactory() throws IOException, SQLException {

        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/" + SUCCESSLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader();
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);


        Configuration pass_into = new Configuration();
//        pass_into.set("SQL_URL","jdbc:hsqldb:file:/opt/dao/testdb");
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");
//        SparkJobDAOImpl impl = JoBDAOFactory.getSparkJobDAO(pass_into);
        JobDAO impl = JoBDAOFactory.getJobDAO(Const.DAO_CLAZZ_SPARK, pass_into);
        impl.add(r);

        ResponseJobModel rmode = impl.findById(1452487986830L, 2);
        System.out.println(rmode.getCpuHour());
        System.out.println(rmode.getExecutor_num());
    }

    @Test
    public void testInitializeFilter() throws IOException, SQLException {
        Configuration pass_into = new Configuration();
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");

        SparkLogFileFilter filter = new SparkLogFileFilter(pass_into);
        long r1 = filter.getLastProcessedFileModifiedTime();
        Assert.assertEquals(100,r1);
        Reader reader = new Reader(pass_into);
        reader.setFilter(filter);
        reader.saveLastProcessedTime(999L);
        long r = filter.getLastProcessedFileModifiedTimeFromDB();
        Assert.assertEquals(r, 999L);
    }


    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }

}
