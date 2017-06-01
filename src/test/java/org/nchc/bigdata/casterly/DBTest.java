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
//    private static String SUCCESSLOG = "application_1491786134915_9837";
    private static String SUCCESSLOG = "application_1491786134915_10290";
//    private static String SUCCESSLOG = "application_1491786134915_9755";
    private static IDatabaseTester databaseTester;


    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
//                "jdbc:hsqldb:file:/opt/dao/testdb", "sa", "");
        testUtil.createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());


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

        ResponseJobModel rmode = impl.findById(1491786134915L, 10290);
        System.out.println(rmode.getCpuHour());
        System.out.println(rmode.getExecutor_num());
    }

    @Ignore
    public void testInitializeFilter() throws IOException, SQLException {
        Configuration pass_into = new Configuration();
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");

        SparkLogFileFilter filter = new SparkLogFileFilter(pass_into);
        filter.saveLastProcessedTime();
        filter.getLastProcessedFileModifiedTimeFromDB();
    }


    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }

}
