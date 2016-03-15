package org.nchc.bigdata.casterly;

import org.apache.log4j.Logger;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.nchc.bigdata.dao.JobDAO;
import org.nchc.bigdata.model.MRJobModel;
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
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/" + filename);
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
        logger.info(impl.calCPUHour(mr));
    }

    @Test
    public void test3() throws Exception {
        impl.add(mr);
        queryDB_APPSUMMARY(databaseTester.getConnection().getConnection());
        queryDB_TASKDETAIL(databaseTester.getConnection().getConnection());
        queryDB_APP_JOIN_TASK(databaseTester.getConnection().getConnection());
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
