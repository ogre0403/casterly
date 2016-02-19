package org.nchc.bigdata.dao;

import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/2.
 */
public interface JobDAO {

    long calCPUHour(JobModel model);
    void add(JobModel model ) throws SQLException;
    void add(List<JobModel> models) throws SQLException;
    List<ResponseJobModel> findByName( String name ) throws SQLException;
    ResponseJobModel findById( long jobId ) throws SQLException;
    long getUsage(long jobId) throws SQLException;


    public void fake() throws SQLException;

}
