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

  /**
   * Indicator whether a job has been processed successfully from the RAW table
   * to the history and index tables. Used to skip this job from the RAW table
   * for the next set of jobs to process.
   */
  public static final String JOB_PROCESSED_SUCCESS_COL = "job_processed_success";
  public static final byte[] JOB_PROCESSED_SUCCESS_COL_BYTES = Bytes
      .toBytes(JOB_PROCESSED_SUCCESS_COL);

  /**
   * The string preceding the job submit time in a job history file.
   */
  public static final String SUBMIT_TIME_PREFIX = "SUBMIT_TIME=\"";

  /**
   * Raw bytes representation of {@link #SUBMIT_TIME_PREFIX};
   */
  public static final byte[] SUBMIT_TIME_PREFIX_BYTES = Bytes
      .toBytes(SUBMIT_TIME_PREFIX);

  public static final String QUOTE = "\"";
  public static final byte[] QUOTE_BYTES = Bytes.toBytes(QUOTE);

  /**
   * The maximum length of a radix 10 string representing a long.
   */
  public static final int MAX_LONG_LENGTH = Long.toString(Long.MAX_VALUE)
      .length();

  public static final String USER_CONF_KEY = "user.name";
  public static final String JOB_NAME_CONF_KEY = "mapred.job.name";

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
}
