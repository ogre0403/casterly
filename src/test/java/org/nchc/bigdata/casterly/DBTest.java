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

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class DBTest {
    private static Logger logger = Logger.getLogger(DBTest.class);
//    private static String SUCCESSLOG = "application_1491786134915_9837";
//    private static String SUCCESSLOG = "application_1491786134915_10290";
    private static String SUCCESSLOG = "application_1491786134915_9755";
    private static IDatabaseTester databaseTester;


    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        databaseTester = new JdbcDatabaseTester(org.hsqldb.jdbcDriver.class.getName(),
                "jdbc:hsqldb:mem:mymemdb", "SA", "");
//                "jdbc:hsqldb:file:/opt/dao/testdb", "sa", "");
        testUtil.createTablesSinceDbUnitDoesNot(databaseTester.getConnection().getConnection());


    }

    @Ignore
    public void testFactory() throws IOException, SQLException {

        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/" + SUCCESSLOG);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Reader reader = new Reader();
        reader.setParser(new SparkLogParserImpl());
        SparkJobModel r = reader.read(br);

        System.out.println(r.getAppStart().getId());

        Configuration pass_into = new Configuration();
//        pass_into.set("SQL_URL","jdbc:hsqldb:file:/opt/dao/testdb");
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");
//        SparkJobDAOImpl impl = JoBDAOFactory.getSparkJobDAO(pass_into);
        JobDAO impl = JoBDAOFactory.getJobDAO(Const.DAO_CLAZZ_SPARK, pass_into);
        impl.add(r);

        ResponseJobModel rmode = impl.findById(1491786134915L, 9755);
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


    @Test
    public void verifyRealLog() throws IOException, SQLException {

        File folder = new File("D:\\ProjectSource\\TEST_AREA\\log\\big");
        File[] listOfFiles = folder.listFiles();
        Arrays.sort(listOfFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
             return   getSeq(o1.getName()) - getSeq(o2.getName());
            }
        });



        BufferedReader br ;
        Reader reader ;


        Configuration pass_into = new Configuration();
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");
        JobDAO impl = JoBDAOFactory.getJobDAO(Const.DAO_CLAZZ_SPARK, pass_into);

        ResponseJobModel rmode ;
        System.out.println(listOfFiles.length);
        System.out.println("==================");

        for (File file : listOfFiles) {

            System.out.print(file.getName());

            InputStream is = new FileInputStream( file);
            br = new BufferedReader(new InputStreamReader(is));
            reader = new Reader();
            reader.setParser(new SparkLogParserImpl());
            SparkJobModel r = reader.read(br);

            impl.add(r);

            rmode = impl.findById(getEpoch(r.getAppStart().getId()), getSeq(r.getAppStart().getId()));
            System.out.println("\t"+ rmode.getCpuHour()*1.95/3600000);
//            System.out.println("num: " + rmode.getExecutor_num());

//            System.out.println("==================");
        }

    }



    protected long getEpoch(String app_id){
        String ids[] = app_id.split("_");
        return Long.parseLong(ids[1]);
    }

    protected int getSeq(String app_id){
        String ids[] = app_id.split("_");
        return Integer.parseInt(ids[2]);
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        databaseTester.onTearDown();
        databaseTester = null;
    }

}
