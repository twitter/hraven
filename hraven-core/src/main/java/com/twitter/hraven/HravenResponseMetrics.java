package com.twitter.hraven;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;
import com.twitter.common.stats.Stats;

/**
 * defines the metrics to be collected
 *
 */
public class HravenResponseMetrics {
  public final static String JOB_API_LATENCY = "JOB_API_LATENCY";
  public static AtomicLong JOB_API_LATENCY_VALUE;

  public final static String FLOW_API_LATENCY = "FLOW_API_LATENCY";
  public static AtomicLong FLOW_API_LATENCY_VALUE;

  public final static String FLOW_STATS_API_LATENCY = "FLOW_STATS_API_LATENCY";
  public static AtomicLong FLOW_STATS_API_LATENCY_VALUE;

  public final static String FLOW_VERSION_API_LATENCY = "FLOW_VERSION_API_LATENCY";
  public static AtomicLong FLOW_VERSION_API_LATENCY_VALUE;

  public final static String FLOW_HBASE_RESULT_SIZE = "FLOW_HBASE_RESULT_SIZE";
  public static AtomicDouble FLOW_HBASE_RESULT_SIZE_VALUE;

  public final static String JOBFLOW_API_LATENCY = "JOBFLOW_API_LATENCY";
  public static AtomicLong JOBFLOW_API_LATENCY_VALUE;

  public final static String TASKS_API_LATENCY = "TASKS_API_LATENCY";
  public static AtomicLong TASKS_API_LATENCY_VALUE;

  public final static String APPVERSIONS_API_LATENCY = "APPVERSIONS_API_LATENCY";
  public static AtomicLong APPVERSIONS_API_LATENCY_VALUE;

  public final static String HDFS_STATS_API_LATENCY = "HDFS_STATS_API_LATENCY";
  public static AtomicLong HDFS_STATS_API_LATENCY_VALUE;

  public final static String HDFS_TIMESERIES_API_LATENCY = "HDFS_TIMESERIES_API_LATENCY";
  public static AtomicLong HDFS_TIMESERIES_API_LATENCY_VALUE;

  public final static String NEW_JOBS_API_LATENCY = "NEW_JOBS_API_LATENCY";
  public static AtomicLong NEW_JOBS_API_LATENCY_VALUE;

  static {
    /** initialize metrics */
    JOB_API_LATENCY_VALUE = Stats.exportLong(JOB_API_LATENCY);
    FLOW_API_LATENCY_VALUE = Stats.exportLong(FLOW_API_LATENCY);
    FLOW_STATS_API_LATENCY_VALUE = Stats.exportLong(FLOW_STATS_API_LATENCY);
    FLOW_VERSION_API_LATENCY_VALUE = Stats.exportLong(FLOW_VERSION_API_LATENCY);
    FLOW_HBASE_RESULT_SIZE_VALUE = Stats.exportDouble(FLOW_HBASE_RESULT_SIZE);
    JOBFLOW_API_LATENCY_VALUE = Stats.exportLong(JOBFLOW_API_LATENCY);
    TASKS_API_LATENCY_VALUE = Stats.exportLong(TASKS_API_LATENCY);
    APPVERSIONS_API_LATENCY_VALUE = Stats.exportLong(APPVERSIONS_API_LATENCY);
    HDFS_STATS_API_LATENCY_VALUE = Stats.exportLong(HDFS_STATS_API_LATENCY);
    HDFS_TIMESERIES_API_LATENCY_VALUE = Stats.exportLong(HDFS_TIMESERIES_API_LATENCY);
    NEW_JOBS_API_LATENCY_VALUE = Stats.exportLong(NEW_JOBS_API_LATENCY);

  }
}
