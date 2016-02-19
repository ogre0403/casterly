package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/17.
 */
public class SparkJobDAOImpl implements JobDAO{

    private static Logger logger = Logger.getLogger(SparkJobDAOImpl.class);

    protected    Connection connection;
    private Statement statement;
    private Configuration conf;

    public SparkJobDAOImpl(Configuration conf){
        this.conf = conf;
    }

    public SparkJobDAOImpl(){}

    @Override
    public long calCPUHour(JobModel model) {
        SparkJobModel.AppEnd appEnd = ((SparkJobModel) model).getAppEnd();
        long endTS = appEnd.getTimestamp();
        long totalTime = 0;
        if (model instanceof SparkJobModel){
            List<SparkJobModel.ExecutorAdded> executors = ((SparkJobModel) model).getExecutorAdd();
            for(SparkJobModel.ExecutorAdded executor : executors){
                totalTime += Long.parseLong(executor.getTime()) - endTS;
            }
            return totalTime;
        }
        logger.warn("Not SparkJobModel, but calculate CPUHour in SparkJobDAO");
        return 0;
    }

    @Override
    public void add(JobModel model) throws SQLException {

    }

    @Override
    public void add(List<JobModel> models) throws SQLException {

    }

    @Override
    public List<ResponseJobModel> findByName(String name) throws SQLException {
        return null;
    }


    @Override
    public ResponseJobModel findById(long id) throws SQLException{
        return null;
    }

    @Override
    public long getUsage(long jobId) throws SQLException {
        return 0;
    }

    @Override
    public void fake() throws SQLException {

        connection = ConnectionFactory.getConnection(conf);
        if(connection != null)
            logger.info("not null");

        String query = "INSERT INTO SCHOOL (EMPID, SALARY) VALUES ('1','100')";
        ResultSet rs = null;
        try {
            connection = ConnectionFactory.getConnection(conf);
            statement = connection.createStatement();
            rs = statement.executeQuery(query);

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
    }

    public void dummy_select() throws SQLException{
        connection = ConnectionFactory.getConnection(conf);
        String query = "SELECT * FROM SCHOOL";
        ResultSet rs = null;
        try {
            connection = ConnectionFactory.getConnection(conf);
            statement = connection.createStatement();
            rs = statement.executeQuery(query);
            if(rs.next()) {
                logger.info(rs.getString("EMPID"));
                logger.info(rs.getString("SALARY"));
            }

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
    }
}
