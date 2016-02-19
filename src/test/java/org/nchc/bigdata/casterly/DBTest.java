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
import org.nchc.bigdata.dao.SparkJobDAOImpl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class DBTest {
    private static Log logger = LogFactory.getLog(SparkLogTest.class);

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
        salCal = new SalaryCalculation();
        salCal.setConnection(databaseTester.getConnection().getConnection());
*/
    }

    private static void createTablesSinceDbUnitDoesNot(Connection connection)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "CREATE TABLE SCHOOL (EMPID VARCHAR(20),SALARY VARCHAR(10))");
        statement.execute();
        statement.close();
    }

    @Test
    public void testFactory() throws IOException, SQLException {
        Configuration pass_into = new Configuration();
//        pass_into.set("SQL_URL","jdbc:hsqldb:file:/opt/dao/testdb");
        pass_into.set("SQL_URL","jdbc:hsqldb:mem:mymemdb");
        pass_into.set("SQL_USER","sa");
        pass_into.set("SQL_PASSWORD", "");
        SparkJobDAOImpl impl = JoBDAOFactory.getSparkJobDAO(pass_into);
        impl.fake();
        impl.dummy_select();
    }



    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }

}
