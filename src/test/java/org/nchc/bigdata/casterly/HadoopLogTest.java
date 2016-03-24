package org.nchc.bigdata.casterly;

import org.apache.log4j.Logger;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.MRJobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.parser.MRLogParserImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Created by 1403035 on 2016/2/2.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HadoopLogTest {
    protected static Logger logger = Logger.getLogger(HadoopLogTest.class);

    public static String filename = "job_1456378629209_0161-1456902161824-u11lss02-streamjob3184777771968013509.jar-1456902176126-2-0-SUCCEEDED-root.u11lss02-1456902167822.jhist";
    public static String failFile = "job_1447654482871_1413-1458542643822-scu240-Training+NBC-1458542677415-0-0-FAILED-root.MR-1458542654035.jhist";
    public static String specialFile = "job_1447654482871_1033-1458116315693-ntunhs-select+_+from+opdte_p+where+id%3D%27cnpay...2000%28Stage-1458116336656-16-0-SUCCEEDED-root.HIVE-1458116326488.jhist";
    public static String ff = "job_1447654482871_1537-1458618025312-scu240-Classify-1458618059087-0-0-FAILED-root.MR-1458618035650.jhist";
    public static String ff2 = "job_1447654482871_1538-1458618548345-scu240-Classify-1458618573991-1-1-SUCCEEDED-root.MR-1458618558722.jhist";

    public static MRJobModel  mr;
    private static IDatabaseTester databaseTester;
    private static JobDAO impl;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
        testUtil.createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());
        impl = testUtil.createDAOImpl(Const.DAO_CLAZZ_MAPREDUCE);
    }

    @Test
    public void test1() throws IOException {
        MRLogParserImpl mrLogParser = new MRLogParserImpl();
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/" + ff2);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        while(null != (line=br.readLine())){
            mrLogParser.parse(line);
        }

        mr = mrLogParser.result();
        Assert.assertEquals(mr.getMapAttemptFinishMap().size(),
                mr.getMapAttemptStartMap().size());
        Assert.assertEquals(mr.getReduceAttemptFinishMap().size(),
                mr.getReduceAttemptStartMap().size());

        for (Map.Entry<String, MRJobModel.TaskAttemptCompleted> entry
                : mr.getMapAttemptFinishMap().entrySet()) {
            logger.info(entry.getValue().getAttemptId());
            logger.info(entry.getValue().getFinishTime());
        }

    }

    @Test
    public void test2() throws SQLException {
        logger.info("==== "+impl.calCPUHour(mr));
    }

    @Test
    public void test3() throws Exception {
        impl.add(mr);
        queryDB_APPSUMMARY(databaseTester.getConnection().getConnection());
        queryDB_TASKDETAIL(databaseTester.getConnection().getConnection());
        queryDB_APP_JOIN_TASK(databaseTester.getConnection().getConnection());
    }

    @Test
    public void test4() throws SQLException {
        ResponseJobModel response = impl.findById(1456378629209L, 161);
        logger.info(response.getCpuHour());
        logger.info(response.getUser());
        logger.info(response.getReduce_num());
        logger.info(response.getMap_num());
    }

    private void queryDB_APPSUMMARY(Connection connection) throws SQLException {
        logger.info("======  APP  ======");
        String query = "SELECT * FROM APP_SUMMARY";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()){
            logger.info(rs.getLong("CPUHOUR"));
            logger.info(rs.getLong("EPOCH"));
            logger.info(rs.getLong("SEQ"));
            logger.info(rs.getString("USER"));
            logger.info(rs.getString("QUEUE"));
            logger.info(rs.getString("JOBNAME"));
            logger.info(rs.getLong("START"));
            logger.info(rs.getLong("FINISH"));
        }
    }

    private void queryDB_TASKDETAIL(Connection connection) throws SQLException {
        logger.info("======   TASK  ======");
        String query = "SELECT * FROM TASK_DETAIL";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()){
            logger.info("-------------");
            logger.info(rs.getLong("EPOCH"));
            logger.info(rs.getLong("SEQ"));
            logger.info(rs.getString("TYPE"));
            logger.info(rs.getLong("TASKID"));
            logger.info(rs.getInt("ATTEMPTID"));
            logger.info(rs.getLong("START"));
            logger.info(rs.getLong("FINISH"));
        }
    }

    private void queryDB_APP_JOIN_TASK (Connection connection) throws SQLException{
        logger.info("======   JOIN  ======");
        String query = "SELECT JOBNAME, TASKID, ATTEMPTID, TYPE " +
                        "FROM APP_SUMMARY INNER JOIN  TASK_DETAIL " +
                        "ON APP_SUMMARY.EPOCH = TASK_DETAIL.EPOCH AND APP_SUMMARY.SEQ = TASK_DETAIL.SEQ " +
                        "WHERE TASK_DETAIL.TYPE = 'R'";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()){
            logger.info(rs.getString("JOBNAME"));
            logger.info(rs.getString("TYPE"));
            logger.info(rs.getLong("TASKID"));
            logger.info(rs.getInt("ATTEMPTID"));
        }
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }



}
