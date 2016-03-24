package org.nchc.bigdata.parser;

import org.nchc.bigdata.model.JobModel;

import java.io.IOException;

/**
 * Created by 1403035 on 2016/2/2.
 */
public interface IParser {
    boolean parse(String line);

    <T extends JobModel> T result();

    void clear();
}
