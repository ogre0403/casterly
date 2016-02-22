package org.nchc.bigdata.casterly;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;
import org.nchc.bigdata.parser.Reader;
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
    private static Log logger = LogFactory.getLog(SparkLogTest.class);
    private static String SUCCESSLOG = "application_1452487986830_0002_1";
    private static IDatabaseTester databaseTester;


    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
//                "jdbc:hsqldb:file:/opt/dao/testdb", "sa", "");

        createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());

//        databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
//        databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
//        databaseTester.onSetup();

        /*
        String inputXml = "<dataset>" + "    <SCHOOL emptitle=\"54601B\" "
                + "       Salary=\"25000\""
                + "       Bonus=\"5000\""
                + "       Increment=\"0\""
                + "       subject=\"mathametics\"/>"
                + "</dataset>";
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(new StringReader(inputXml));
        databaseTester.setDataSet(dataSet);
        databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
        databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
        databaseTester.onSetup();
*/
    }

    private static void createTablesSinceDbUnitDoesNot(Connection connection)
            throws SQLException {
        PreparedStatement app_sum = connection.prepareStatement(
                "CREATE TABLE APP_SUMMARY (" +
                "EPOCH BIGINT, SEQ SMALLINT, TYPE CHAR(10), " +
                "USER CHAR(16), JOBNAME CHAR(255), QUEUE CHAR(32), " +
                "START BIGINT , FINISH BIGINT, CPUHOUR BIGINT)");
        app_sum.execute();
        app_sum.close();

        PreparedStatement task_sum = connection.prepareStatement(
                "CREATE TABLE TASK_SUMMARY (" +
                "EPOCH BIGINT, SEQ SMALLINT, TYPE CHAR(10), "+
                "MAP INT, REDUCE INT, "+
                "EXECUTOR INT, TASK INT) ");
        task_sum.execute();
        task_sum.close();

        PreparedStatement task_detail = connection.prepareStatement(
                "CREATE TABLE EXECUTOR_DETAIL (" +
                "EPOCH BIGINT, SEQ SMALLINT, ID SMALLINT, START BIGINT)");
        task_detail.execute();
        task_detail.close();
    }

    @Test
    public void testFactory() throws IOException, SQLException {

        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/spark/" + SUCCESSLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader(new Configuration());
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
        long a = impl.getUsage(1452487986830L, 2);
        System.out.println(a);

        ResponseJobModel rmode = impl.findById(1452487986830L, 2);
        System.out.println(rmode.getCpuHour());
    }



    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }

}
