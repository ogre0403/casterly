package org.nchc.bigdata.model;

import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Util;

import java.io.*;

/**
 * Created by 1403035 on 2016/2/17.
 */
public abstract class JobModel implements Serializable {
    private static Logger logger = Logger.getLogger(JobModel.class);
    public JobModel deepClone() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (JobModel) ois.readObject();
        } catch (IOException e) {
            logger.error(Util.traceString(e));
            return null;
        } catch (ClassNotFoundException e) {
            logger.error(Util.traceString(e));
            return null;
        }
    }

    public abstract void clean();

}
