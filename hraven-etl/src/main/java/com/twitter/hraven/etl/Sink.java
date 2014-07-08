package com.twitter.hraven.etl;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import com.twitter.hraven.mapreduce.GraphiteOutputFormat;
import com.twitter.hraven.mapreduce.HbaseOutputFormat;
import com.twitter.hraven.mapreduce.JobFileTableMapper;

public enum Sink {

  HBASE {

    @Override
    public void configureJob(Job job) {
      MultipleOutputs.addNamedOutput(job, name(), HbaseOutputFormat.class,
        JobFileTableMapper.getOutputKeyClass(), JobFileTableMapper.getOutputValueClass());
    }

  },

  GRAPHITE {

    @Override
    public void configureJob(Job job) {
      MultipleOutputs.addNamedOutput(job, name(), GraphiteOutputFormat.class,
        JobFileTableMapper.getOutputKeyClass(), JobFileTableMapper.getOutputValueClass());
    }

  };

  public abstract void configureJob(Job job);

}
