package org.nchc.bigdata.casterly;

/**
 * Created by 1403035 on 2016/2/22.
 */
public class Const {
    public static final String SQL_USER = "SQL_USER";
    public static final String SQL_PASSWORD = "SQL_PASSWORD";
    public static final String SQL_URL = "SQL_URL";

    public static final String SQL_TEMPLATE_ADD_JOB = "INSERT INTO " +
            "APP_SUMMARY " +
            "(EPOCH, SEQ, TYPE, USER, JOBNAME, QUEUE, START, FINISH, CPUHOUR) " +
            "VALUES " +
            "(%d, %d, \'%s\', \'%s\', \'%s\', \'%s\', %d, %d, %d)";
    public static final String SQL_TEMPLATE_USAGE = "SELECT CPUHOUR FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";
    public static final String SQL_TEMPLATE_JOB_SUM = "SELECT * FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";


    public static final String DAO_CLAZZ="DAO_CLAZZ";
    public static final String DAO_CLAZZ_SPARK = "org.nchc.bigdata.dao.SparkJobDAOImpl";
    public static final String DAO_CLAZZ_MAPREDUCE = "org.nchc.bigdata.dao.MRJobDAOImpl";
}
