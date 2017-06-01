package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.nchc.bigdata.dao.JoBDAOFactory;
import org.nchc.bigdata.dao.JobDAO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/4.
 */
public class testUtil {
    public static void putToHDFS(String file, String dest_dir, FileSystem fs, Configuration conf) throws IOException {
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream( file);
        OutputStream os = fs.create(new Path(dest_dir + file));
        IOUtils.copyBytes(is, os, conf);
        is.close();
        os.close();
    }

    public static void delFromHDFS(String path, MiniDFSCluster mini_dfs) throws IOException {
        DistributedFileSystem fs = mini_dfs.getFileSystem();
        fs.delete(new Path(path), false);
    }

    public static void createTablesSinceDbUnitDoesNot(Connection connection)
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
                        "TASKID BIGINT, ATTEMPTID SMALLINT, " +
                        "START BIGINT, FINISH BIGINT) ");
        task_detail.execute();
        task_detail.close();

        PreparedStatement executor_detail = connection.prepareStatement(
                "CREATE TABLE EXECUTOR_DETAIL (" +
                        "EPOCH BIGINT, SEQ SMALLINT, ID SMALLINT, START BIGINT, FINISH BIGINT)");
        executor_detail.execute();
        executor_detail.close();

        PreparedStatement last_processed = connection.prepareStatement(
                "CREATE  TABLE  LAST_PROCESSED (" +
                        "ID INT, LAST BIGINT)");
        last_processed.execute();
        last_processed = connection.prepareStatement(
                "INSERT INTO LAST_PROCESSED (ID, LAST) VALUES (1,0)");
        last_processed.execute();
        last_processed.close();
    }

    public static JobDAO createDAOImpl(String clazz){
        Configuration pass_into = new Configuration();
        pass_into.set(Const.SQL_URL,"jdbc:hsqldb:mem:mymemdb");
        pass_into.set(Const.SQL_USER, "sa");
        pass_into.set(Const.SQL_PASSWORD, "");
        return JoBDAOFactory.getJobDAO(clazz, pass_into);
    }

}
