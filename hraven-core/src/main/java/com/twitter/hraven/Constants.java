/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.hraven;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class Constants {

  public static final String PROJECT_NAME = "hraven";
  
  // HBase constants

  // separator character used between key components
  public static final char SEP_CHAR = '!';
  public static final String SEP = "" + SEP_CHAR;
  public static final byte[] SEP_BYTES = Bytes.toBytes(SEP);

  // common default values
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] ZERO_INT_BYTES = Bytes.toBytes(0);
  public static final byte[] ZERO_LONG_BYTES = Bytes.toBytes(0L);
  public static final byte[] ZERO_SINGLE_BYTE = new byte[]{ 0 };

  public static final String UNKNOWN = "";

  public static boolean IS_DEV = false;
  public static String PREFIX = IS_DEV ? "dev." : "";

  /* **** Table names **** */
  public static String HISTORY_TABLE = PREFIX + "job_history";
  public static byte[] HISTORY_TABLE_BYTES = Bytes.toBytes(HISTORY_TABLE);

  public static String HISTORY_TASK_TABLE = HISTORY_TABLE + "_task";
  public static byte[] HISTORY_TASK_TABLE_BYTES = Bytes
      .toBytes(HISTORY_TASK_TABLE);

  public static String HISTORY_BY_JOBID_TABLE = HISTORY_TABLE + "-by_jobId";
  public static byte[] HISTORY_BY_JOBID_TABLE_BYTES = Bytes
      .toBytes(HISTORY_BY_JOBID_TABLE);

  public static String HISTORY_APP_VERSION_TABLE = HISTORY_TABLE
      + "_app_version";
  public static byte[] HISTORY_APP_VERSION_TABLE_BYTES = Bytes
      .toBytes(HISTORY_APP_VERSION_TABLE);

  public static String HISTORY_RAW_TABLE = HISTORY_TABLE + "_raw";
  public static byte[] HISTORY_RAW_TABLE_BYTES = Bytes
      .toBytes(HISTORY_RAW_TABLE);

  public static final String JOB_FILE_PROCESS_TABLE = PREFIX
      + "job_history_process";
  public static final byte[] JOB_FILE_PROCESS_TABLE_BYTES = Bytes
      .toBytes(JOB_FILE_PROCESS_TABLE);

  public static final String FLOW_QUEUE_TABLE = PREFIX + "flow_queue";
  public static final byte[] FLOW_QUEUE_TABLE_BYTES = Bytes.toBytes(FLOW_QUEUE_TABLE);

  public static final String FLOW_EVENT_TABLE = PREFIX + "flow_event";
  public static final byte[] FLOW_EVENT_TABLE_BYTES = Bytes.toBytes(FLOW_EVENT_TABLE);

  public static final String INFO_FAM = "i";
  public static final byte[] INFO_FAM_BYTES = Bytes.toBytes(INFO_FAM);

  public static final String RAW_FAM = "r";
  public static final byte[] RAW_FAM_BYTES = Bytes.toBytes(RAW_FAM);

  /** Column qualifier prefix to namespace job configuration properties */
  public static final String JOB_CONF_COLUMN_PREFIX = "c";
  public static byte[] JOB_CONF_COLUMN_PREFIX_BYTES = Bytes
      .toBytes(JOB_CONF_COLUMN_PREFIX);

  /** Column qualifier prefix to namespace counter data */
  public static final String COUNTER_COLUMN_PREFIX = "g";
  public static final byte[] COUNTER_COLUMN_PREFIX_BYTES = Bytes
      .toBytes(COUNTER_COLUMN_PREFIX);

  /** Column qualifier prefix to namespace map-specific counter data */
  public static final String MAP_COUNTER_COLUMN_PREFIX = "gm";
  public static final byte[] MAP_COUNTER_COLUMN_PREFIX_BYTES = Bytes
      .toBytes(MAP_COUNTER_COLUMN_PREFIX);

  /** Column qualifier prefix to namespace reduce-specific counter data */
  public static final String REDUCE_COUNTER_COLUMN_PREFIX = "gr";
  public static final byte[] REDUCE_COUNTER_COLUMN_PREFIX_BYTES = Bytes
      .toBytes(REDUCE_COUNTER_COLUMN_PREFIX);

  public static final String JOBCONF_COL = "jobconf";
  public static final byte[] JOBCONF_COL_BYTES = Bytes.toBytes(JOBCONF_COL);

  public static final String JOBCONF_LAST_MODIFIED_COL = JOBCONF_COL
      + "_last_modified";
  public static final byte[] JOBCONF_LAST_MODIFIED_COL_BYTES = Bytes
      .toBytes(JOBCONF_LAST_MODIFIED_COL);

  public static final String JOBCONF_FILENAME_COL = JOBCONF_COL + "_filename";
  public static final byte[] JOBCONF_FILENAME_COL_BYTES = Bytes
      .toBytes(JOBCONF_FILENAME_COL);

  public static final String JOBHISTORY_COL = "jobhistory";
  public static final byte[] JOBHISTORY_COL_BYTES = Bytes
      .toBytes(JOBHISTORY_COL);

  public static final String JOBHISTORY_LAST_MODIFIED_COL = JOBHISTORY_COL
      + "_last_modified";
  public static final byte[] JOBHISTORY_LAST_MODIFIED_COL_BYTES = Bytes
      .toBytes(JOBHISTORY_LAST_MODIFIED_COL);

  public static final String JOBHISTORY_FILENAME_COL = JOBHISTORY_COL
      + "_filename";
  public static final byte[] JOBHISTORY_FILENAME_COL_BYTES = Bytes
      .toBytes(JOBHISTORY_FILENAME_COL);

  /** Column qualifer used to flag job_history_raw records for reprocessing */
  public static final String RAW_COL_REPROCESS = "reprocess";
  public static final byte[] RAW_COL_REPROCESS_BYTES = Bytes.toBytes(RAW_COL_REPROCESS);

  public static final String SUBMIT_TIME_COL = "submit_time";
  public static final byte[] SUBMIT_TIME_COL_BYTES = Bytes.toBytes(SUBMIT_TIME_COL);

  public static final String ROWKEY_COL = "rowkey";
  public static final byte[] ROWKEY_COL_BYTES = Bytes.toBytes(ROWKEY_COL);

  public static final String RECORD_TYPE_COL = "rec_type";
  public static final byte[] RECORD_TYPE_COL_BYTES = Bytes
      .toBytes(RECORD_TYPE_COL);

  public static final String MIN_MOD_TIME_MILLIS_COLUMN = "min_mod_millis";
  public static final byte[] MIN_MOD_TIME_MILLIS_COLUMN_BYTES = Bytes
      .toBytes(MIN_MOD_TIME_MILLIS_COLUMN);

  public static final String PROCESSED_JOB_FILES_COLUMN = "processed_job_files";
  public static final byte[] PROCESSED_JOB_FILES_COLUMN_BYTES = Bytes
      .toBytes(PROCESSED_JOB_FILES_COLUMN);

  public static final String PROCESS_FILE_COLUMN = "processing_directory";
  public static final byte[] PROCESS_FILE_COLUMN_BYTES = Bytes
      .toBytes(PROCESS_FILE_COLUMN);

  public static final String PROCESSING_STATE_COLUMN = "processing_state";
  public static final byte[] PROCESSING_STATE_COLUMN_BYTES = Bytes
      .toBytes(PROCESSING_STATE_COLUMN);

  public static final String VERSION_COLUMN = "version";
  public static final byte[] VERSION_COLUMN_BYTES = Bytes
      .toBytes(VERSION_COLUMN);

  public static final String FRAMEWORK_COLUMN = "framework";
  public static final byte[] FRAMEWORK_COLUMN_BYTES = Bytes
      .toBytes(FRAMEWORK_COLUMN);
  
  public static final String MIN_JOB_ID_COLUMN = "min_jobid";
  public static final byte[] MIN_JOB_ID_COLUMN_BYTES = Bytes
      .toBytes(MIN_JOB_ID_COLUMN);
  
  public static final String MAX_JOB_ID_COLUMN = "max_jobid";
  public static final byte[] MAX_JOB_ID_COLUMN_BYTES = Bytes
      .toBytes(MAX_JOB_ID_COLUMN);

  // job details related counter stats
  public static final String FILESYSTEM_COUNTERS = "FileSystemCounters";
  public static final String FILES_BYTES_READ = "FILE_BYTES_READ";
  public static final String FILES_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";
  public static final String HDFS_BYTES_READ = "HDFS_BYTES_READ";
  public static final String HDFS_BYTES_WRITTEN = "HDFS_BYTES_WRITTEN";
  /* TODO: update for 2.0.3+, this class is now deprecated */
  public static final String JOBINPROGRESS_COUNTER = "org.apache.hadoop.mapred.JobInProgress$Counter";
  /* TODO: update for 2.0.3+, this class is now deprecated */
  public static final String TASK_COUNTER = "org.apache.hadoop.mapred.Task$Counter";
  public static final String SLOTS_MILLIS_MAPS  = "SLOTS_MILLIS_MAPS";
  public static final String SLOTS_MILLIS_REDUCES = "SLOTS_MILLIS_REDUCES";
  public static final String REDUCE_SHUFFLE_BYTES = "REDUCE_SHUFFLE_BYTES";

  /** used to indicate how expensive a job is in terms of memory and time taken*/
  public static final String MEGABYTEMILLIS = "megabytemillis" ;
  public static final byte[] MEGABYTEMILLIS_BYTES = Bytes.toBytes(MEGABYTEMILLIS);

  /** use this value to indicate fields that were not found */
  public static final Long NOTFOUND_VALUE = -1L;

  // job details related counter sub group in hadoop 2
  public static final String FILESYSTEM_COUNTER_HADOOP2 = "org.apache.hadoop.mapreduce.FileSystemCounter";
  public static final String JOB_COUNTER_HADOOP2 = "org.apache.hadoop.mapreduce.JobCounter";
  public static final String TASK_COUNTER_HADOOP2 = "org.apache.hadoop.mapreduce.TaskCounter";

  
  /**
   * Indicator whether a job has been processed successfully from the RAW table
   * to the history and index tables. Used to skip this job from the RAW table
   * for the next set of jobs to process.
   */
  public static final String JOB_PROCESSED_SUCCESS_COL = "job_processed_success";
  public static final byte[] JOB_PROCESSED_SUCCESS_COL_BYTES = Bytes
      .toBytes(JOB_PROCESSED_SUCCESS_COL);

  /**
   * The string preceding the job submit time in an hadoop1 job history file.
   */
  public static final String SUBMIT_TIME_PREFIX = "SUBMIT_TIME=\"";

  /**
   * Raw bytes representation of {@link #SUBMIT_TIME_PREFIX};
   */
  public static final byte[] SUBMIT_TIME_PREFIX_BYTES = Bytes
      .toBytes(SUBMIT_TIME_PREFIX);

  /**
   * The string representing the job submit event in a hadoop2 job history file.
   */
  public static final String JOB_SUBMIT_EVENT = "{\"type\":\"JOB_SUBMITTED";

  /**
   * Raw bytes representation of {@link #JOB_SUBMIT_EVENT};
   */
  public static final byte[] JOB_SUBMIT_EVENT_BYTES = Bytes
      .toBytes(JOB_SUBMIT_EVENT);

  /**
   * The string representing the submit time in a hadoop2 job history file.
   */
  public static final String SUBMIT_TIME_PREFIX_HADOOP2 = "\"submitTime\":";

  /**
   * Raw bytes representation of {@link #SUBMIT_TIME_PREFIX_HADOOP2};
   */
  public static final byte[] SUBMIT_TIME_PREFIX_HADOOP2_BYTES = Bytes
      .toBytes(SUBMIT_TIME_PREFIX_HADOOP2);

  /**
   * length of string that contains a unix timestamp in milliseconds
   * this length will be correct till Sat, 20 Nov 2286 which is
   * 9999999999999 in epoch time
   */
  public static final int EPOCH_TIMESTAMP_STRING_LENGTH = 13;

  /**
   * an approximation for job run time in milliseconds
   *
   * used in estimating job start time
   * when the submit time for a job can't be figured out
   * https://github.com/twitter/hraven/issues/67
   */
  public static final int AVERGAE_JOB_DURATION = 3600000;

  public static final String QUOTE = "\"";
  public static final byte[] QUOTE_BYTES = Bytes.toBytes(QUOTE);

  /**
   * The maximum length of a radix 10 string representing a long.
   */
  public static final int MAX_LONG_LENGTH = Long.toString(Long.MAX_VALUE)
      .length();

  public static final String USER_CONF_KEY = "user.name";
  public static final String USER_CONF_KEY_HADOOP2 = "mapreduce.job.user.name";

  /**
   *  use this config setting to define an
   *  hadoop-version-independent property for queuename
   */
  public static final String HRAVEN_QUEUE = "queue";

  /** raw bytes representation of the queue parameter */
  public static final byte[] HRAVEN_QUEUE_BYTES = Bytes.toBytes(HRAVEN_QUEUE);

  /**
   * following are different config parameters that are in use
   * to define a queue for a job
   */
  public static final String QUEUENAME_HADOOP2 = "mapreduce.job.queuename";
  public static final String FAIR_SCHEDULER_POOLNAME_HADOOP1 = "mapred.fairscheduler.pool";
  public static final String CAPACITY_SCHEDULER_QUEUENAME_HADOOP1 = "mapred.job.queue.name";

  /**
   *  use this when queue name cannot be determined, for example in FIFO scheduler
   */
  public static final String DEFAULT_QUEUENAME = "DEFAULT_QUEUE";

  /**
   * this setting is a resultant of a bug in hadoop2
   * there should not exist a value of a string "default" for queuename
   * it should always default to the user name
   */
  public static final String DEFAULT_VALUE_QUEUENAME = "default";

  public static final String JOB_NAME_CONF_KEY = "mapred.job.name";
  public static final String JOB_NAME_HADOOP2_CONF_KEY = "mapreduce.job.name";

  public static final String PIG_CONF_KEY = "pig.version"; // used to detect a
                                                           // pig job
  public static final String APP_NAME_CONF_KEY = "batch.desc";

  /**
   * Added as part of PIG-2587
   */
  public static final String PIG_VERSION_CONF_KEY = "pig.logical.plan.signature";
  public static final String PIG_RUN_CONF_KEY = "pig.script.submitted.timestamp";
  public static final String PIG_LOG_FILE_CONF_KEY = "pig.logfile";

  public static final String CASCADING_FLOW_ID_CONF_KEY = "cascading.flow.id";

  public static final String CASCADING_APP_NAME_CONF_KEY = "scalding.flow.class.name";
  public static final String CASCADING_VERSION_CONF_KEY = "scalding.flow.class.signature";
  public static final String CASCADING_RUN_CONF_KEY = "scalding.flow.submitted.timestamp";
  public static final String CASCADING_APP_ID_CONF_KEY = "cascading.app.id";
  
  public static final String MR_RUN_CONF_KEY = "mapred.app.submitted.timestamp";

  /**
   * hadoop2 memory mb for container size
   * for map, reduce and AM containers
   */
  public static final String MAP_MEMORY_MB_CONF_KEY = "mapreduce.map.memory.mb";
  public static final long DEFAULT_MAP_MEMORY_MB = 1536L;
  public static final String REDUCE_MEMORY_MB_CONF_KEY = "mapreduce.reduce.memory.mb";
  public static final String AM_MEMORY_MB_CONF_KEY = "yarn.app.mapreduce.am.resource.mb";

  /** yarn scheduler min mb is 1G */
  public static final String YARN_SCHEDULER_MIN_MB = "yarn.scheduler.minimum-allocation-mb";
  public static final long DEFAULT_YARN_SCHEDULER_MIN_MB = 1024;

  /**
   * hadoop1 memory conf keys
   */
  public static final String JAVA_CHILD_OPTS_CONF_KEY = "mapred.child.java.opts";
  /** default xmx size is 1 GB as per 
   * http://docs.oracle.com/javase/6/docs/technotes/guides/vm/gc-ergonomics.html
   */
  public static final Long DEFAULT_XMX_SETTING = 1024L;
  public static final String DEFAULT_XMX_SETTING_STR = Long.toString(DEFAULT_XMX_SETTING) + "M";

  /**
   * Timestamp format used to create processing directories
   */
  public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat(
      "yyyyMMddHHmmss");

  // Initialize to use UTC
  static {
    TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /**
   * Regex to parse a job file name. First back-ref is JT name, second one if
   * job-id
   * <p>
   * For example,
   * cluster-jt.identifier.example.com_1333569494142_job_201204041958_150125_conf
   * .xml
   * Regex should also be able to parse new format of history files since
   * MAPREDUCE-323 ( as packaged in cdh3u5)
   * For example: history filename is
   * job_201306192120_0003_1371677828795_hadoop_word+count
   * and conf file name is
   * job_201306192120_0003_1371677828795_hadoop_conf.xml
   * in hadoop 2.0, job history file names are named as
   * job_1374258111572_0003-1374260622449-userName1-TeraGen-1374260635219-2-0-SUCCEEDED-default.jhist
   */
  public static final String JOB_FILENAME_PATTERN_REGEX = ".*(job_[0-9]*_[0-9]*)(-|_)([0-9]*[aA-zZ]*)*(.*)$";

  // JobHistory file name parsing related
  public static final String JOB_CONF_FILE_END = "(.*)(conf.xml)$";

  /**
   * Regex to parse pig logfile name such as
   * "/var/log/pig/pig_1334818693838.log"
   */
  public static final String PIG_LOGFILE_PATTERN_REGEX = "^.*pig_([0-9]*).log$";

  /**
   * Used for legacy jobs that don't have batch.desc set. Should convert from
   * something like this:
   * PigLatin:daily_job:daily_2012/06/22-00:00:00_to_2012/06/23-00:00:00 top this:
   * PigJobDescFactory.SCHEDULED_PREFIX + "daily_job:daily"
   */
  public static final String PIG_SCHEDULED_JOBNAME_PATTERN_REGEX = "^PigLatin:([^:]*(:.*?)+)_([0-9]{4})/.*";

  /**
   * Used to pass the cluster name from the tool to the RecordReader.
   */
  public static final String CLUSTER_JOB_CONF_KEY = "jobhistory.cluster";
  
  /**
   * Used to pass boolean to mappers to indicate that items are to be reprocessed.
   */
  public static final String FORCE_REPROCESS_CONF_KEY = "force.reprocess";

  /** milliseconds in 1 day */
  public static final long MILLIS_ONE_DAY = 86400000L;

  /**
   * number of milli seconds in 30 days
   */
  public static final long THIRTY_DAYS_MILLIS = MILLIS_ONE_DAY * 30L;

  /** used to indicate the cost of a job is in terms of currency units */
  public static final String JOBCOST = "jobcost" ;

  public static final byte[] JOBCOST_BYTES = Bytes.toBytes(JOBCOST);

  /** hdfs location where the properties file is placed */
  public static final String COST_PROPERTIES_HDFS_DIR = "/user/hadoop/hraven/conf/";

  /** Cost properties file name */
  public static final String COST_PROPERTIES_FILENAME = "hRavenCostDetails.properties";

  /** name of the type of machine the job ran on */
  public static final String HRAVEN_MACHINE_TYPE = "hraven.machinetype.name";

  /** name of the properties file used for cluster to cluster identifier mapping */
  public static final String HRAVEN_CLUSTER_PROPERTIES_FILENAME = "hRavenClusters.properties";
}
