package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;

import java.sql.Connection;
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
    public void add(List<JobModel> models) {
        connection = ConnectionFactory.getConnection(conf);
        try {
            for (JobModel model : models) {
                if (!(model instanceof SparkJobModel)) {
                    logger.warn("Not SparkJobModel. Can not add by " + this.getClass().getSimpleName() + " class");
                }
                String job_id = ((SparkJobModel) model).getAppStart().getId();
                // skip spark job using local mode
                if(job_id.startsWith("local")) {
                    logger.warn("Skip local mode spark Job, JobID: " + job_id);
                    continue;
                }
                // add basic Application info
                long epoch = getEpoch(job_id);
                long seq = getSeq(job_id);
                long cpuhour = calCPUHour(model);
                String user = ((SparkJobModel) model).getAppStart().getUser();
                String jobName = ((SparkJobModel) model).getAppStart().getName();
                long start = ((SparkJobModel) model).getAppStart().getTimestamp();
                long finish = ((SparkJobModel) model).getAppEnd().getTimestamp();

                // add app summary
                addAppSummary(connection, epoch, seq, user, jobName, "spark", start, finish, cpuhour, model);
            }
        } finally{
            DBUtil.close(connection);
        }
    }

    @Override
    public boolean addOtherDetail(JobModel jobModel) {
        // add executor info
        String job_id = ((SparkJobModel) jobModel).getAppStart().getId();
        long epoch = getEpoch(job_id);
        long seq = getSeq(job_id);
        return addExecutorDetail(connection, ((SparkJobModel) jobModel).getExecutorAdd(), epoch, seq);
    }

    private boolean addExecutorDetail(Connection connection,
                                      List<SparkJobModel.ExecutorAdded> executorList,
                                      long epoch, long seq){
        PreparedStatement prepStatAddExec = null;
        try {
            prepStatAddExec = connection.prepareStatement(Const.SQL_TEMPLATE_ADD_EXECUTOR);
        }catch (SQLException sqle){
            logger.error(Util.traceString(sqle));
            return false;
        }

        try{
            for(SparkJobModel.ExecutorAdded executor : executorList){
                long executor_start = Long.parseLong(executor.getTime());
                int executor_id = Integer.parseInt(executor.getId());
                    prepStatAddExec.setLong(1, epoch);
                    prepStatAddExec.setLong(2, seq);
                    prepStatAddExec.setInt(3, executor_id);
                    prepStatAddExec.setLong(4, executor_start);
                    prepStatAddExec.executeUpdate();
            }
        }catch (SQLException sqle){
            logger.error(Util.traceString(sqle));
                return false;
        }finally {
            DBUtil.close(prepStatAddExec);
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
