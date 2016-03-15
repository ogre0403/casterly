package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/2.
 */
public abstract class JobDAO {

    private static Logger logger = Logger.getLogger(JobDAO.class);

    protected Connection connection;
    protected Statement statement;
    protected Configuration conf;

    public abstract long calCPUHour(JobModel model);
    public abstract boolean add(List<JobModel> models) throws SQLException;
    public abstract List<ResponseJobModel> findByTime(long start, long end) throws SQLException;
    public abstract ResponseJobModel findById( long epoch, int seq ) throws SQLException;

    public boolean add(JobModel model ) throws SQLException{
        ArrayList<JobModel> single = new ArrayList<>(1);
        single.add(model);
        return add(single);
    }

    protected long getEpoch(String app_id){
        String ids[] = app_id.split("_");
        return Long.parseLong(ids[1]);
    }

    protected int getSeq(String app_id){
        String ids[] = app_id.split("_");
        return Integer.parseInt(ids[2]);
    }


    public long getUsage(long epoch, int seq) throws SQLException{
        ResultSet rs = null;
        long result = 0L;
        String query = String.format(Const.SQL_TEMPLATE_USAGE, epoch, seq);
        try{
            connection = ConnectionFactory.getConnection(conf);
            statement = connection.createStatement();
            rs = statement.executeQuery(query);
            if(rs.next()) {
                result = rs.getLong("CPUHOUR");
            }
        }finally{
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return result;
    }

    protected ResultSet accessDbById(long epoch, int seq) throws SQLException{
        String query = String.format(Const.SQL_TEMPLATE_JOB_SUM, epoch, seq);
        connection = ConnectionFactory.getConnection(conf);
        statement = connection.createStatement();
        return statement.executeQuery(query);

    }


}
