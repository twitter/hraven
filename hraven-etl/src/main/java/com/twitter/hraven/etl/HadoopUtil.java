package com.twitter.hraven.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.twitter.hraven.Constants;

public class HadoopUtil {
  public static void setTmpJars(String libPathConf, Configuration conf) throws IOException {
    StringBuilder tmpjars = new StringBuilder();
    if (conf.get(libPathConf) != null) {
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] files = fs.listStatus(new Path(conf.get(libPathConf)));
      if (files != null) {
        for (FileStatus file : files) {
          if (!tmpjars.toString().isEmpty()) tmpjars = tmpjars.append(",");
          tmpjars = tmpjars.append(file.getPath());
        }
        conf.set(Constants.HADOOP_TMP_JARS_CONF, tmpjars.toString());
      }
    }
  }
}
