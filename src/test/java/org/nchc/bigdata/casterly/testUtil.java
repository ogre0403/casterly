package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

}
