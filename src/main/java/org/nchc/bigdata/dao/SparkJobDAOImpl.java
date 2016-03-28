package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/17.
 */
public class SparkJobDAOImpl extends JobDAO{

    private static Logger logger = Logger.getLogger(SparkJobDAOImpl.class);

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
                totalTime = totalTime + (endTS - Long.parseLong(executor.getTime()));
            }
            return totalTime;
        }
        logger.warn("Not SparkJobModel, but calculate CPUHour in SparkJobDAO");
        return 0;
    }


    @Override
    public boolean add(List<JobModel> models) throws SQLException {
        connection = ConnectionFactory.getConnection(conf);
        PreparedStatement prepStatAddJob = connection.prepareStatement(Const.SQL_TEMPLATE_ADD_JOB);
        PreparedStatement prepStatAddExec = connection.prepareStatement(Const.SQL_TEMPLATE_ADD_EXECUTOR);
        try {
            for (JobModel model : models) {
                if (!(model instanceof SparkJobModel)) {
                    logger.warn("Not SparkJobModel. Can not add by " + this.getClass().getSimpleName() + " class");
                    return false;
                }

                // add basic Application info
                long epoch = getEpoch(((SparkJobModel) model).getAppStart().getId());
                long seq = getSeq(((SparkJobModel) model).getAppStart().getId());
                long cpuhour = calCPUHour(model);
                String user = ((SparkJobModel) model).getAppStart().getUser();
                String jobName = ((SparkJobModel) model).getAppStart().getName();
                long start = ((SparkJobModel) model).getAppStart().getTimestamp();
                long finish = ((SparkJobModel) model).getAppEnd().getTimestamp();


                prepStatAddJob.setLong(1, epoch);
                prepStatAddJob.setLong(2, seq);
                prepStatAddJob.setString(3, user);
                prepStatAddJob.setString(4, jobName);
                prepStatAddJob.setString(5, "spark");
                prepStatAddJob.setLong(6, start);
                prepStatAddJob.setLong(7,finish);
                prepStatAddJob.setLong(8,cpuhour);

                prepStatAddJob.executeUpdate();

                // add executor info
                List<SparkJobModel.ExecutorAdded> executorList = ((SparkJobModel) model).getExecutorAdd();
                for(SparkJobModel.ExecutorAdded executor : executorList){
                    long executor_start = Long.parseLong(executor.getTime());
                    int executor_id = Integer.parseInt(executor.getId());
                    prepStatAddExec.setLong(1,epoch);
                    prepStatAddExec.setLong(2,seq);
                    prepStatAddExec.setInt(3,executor_id);
                    prepStatAddExec.setLong(4,executor_start);
                    prepStatAddExec.executeUpdate();
                }
            }
        }finally{
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return true;
    }

    @Override
    public List<ResponseJobModel> findByTime(long start, long end) throws SQLException {

        return null;
    }


    @Override
    public ResponseJobModel findById(long epoch, int seq) throws SQLException{
        ResultSet rs = accessDbById(epoch, seq);
        ResponseJobModel resultModel = new ResponseJobModel();

        try{
            // create ResponseJobModel from query result
            if(rs.next()) {
                resultModel.setCpuHour(rs.getLong("CPUHOUR"));
                resultModel.setUser(rs.getString("USER"));
                resultModel.setQueue(rs.getString("QUEUE"));
                resultModel.setJobName(rs.getString("JOBNAME"));
                resultModel.setSubmit_time(rs.getLong("START"));
                resultModel.setFinish_time(rs.getLong("FINISH"));
                resultModel.setTask_num(0L);
            }
            // count the number of executor
            rs = statement.executeQuery(String.format(Const.SQL_TEMPLATE_EXECOUNT_COUNT, epoch,seq));
            if(rs.next()){
                resultModel.setExecutor_num(rs.getInt("executor_count"));
            }

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return resultModel;
    }

}
