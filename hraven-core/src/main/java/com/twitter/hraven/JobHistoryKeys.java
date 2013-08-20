package com.twitter.hraven;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Contains the extract of the keys enum from
 * {@link org.apache.hadoop.mapreduce.JobHistoryCopy}  class
 * for hadoop1
 *
 * Job history files contain key="value" pairs in hadoop1,
 * where keys belong to this enum
 * This class acts as a global namespace for all keys.
 *
 * *TODO*
 * When job history keys are added/removed from original enum,
 * this class may need to be modified as well.
 * (just like the same goes for
 * {@link org.apache.hadoop.mapreduce.JobHistoryCopy} as well
 * in hraven etl for hadoop1)
 *
 */

public enum JobHistoryKeys {
	JOBTRACKERID(String.class, null),
	START_TIME(Long.class, "startTime"),
	FINISH_TIME(Long.class, "finishTime"),
	JOBID(String.class, "jobid"),
	JOBNAME(String.class, "jobName"),
	USER(String.class, "userName"),
	JOBCONF(String.class, "jobConfPath"),
	SUBMIT_TIME(Long.class, "submitTime"),
	LAUNCH_TIME(Long.class, "launchTime"),
	TOTAL_MAPS(Long.class, "totalMaps"),
	TOTAL_REDUCES(Long.class, "totalReduces"),
	FAILED_MAPS(Long.class, "failedMaps"),
	FAILED_REDUCES(Long.class, "failedReduces"),
	FINISHED_MAPS(Long.class, "finishedMaps"),
	FINISHED_REDUCES(Long.class, "finishedReduces"),
	JOB_STATUS(String.class, "jobStatus"),
	TASKID(String.class, "taskid"),
	HOSTNAME(String.class, "hostname"),
	TASK_TYPE(String.class, "taskType"),
	ERROR(String.class, "error"),
	TASK_ATTEMPT_ID(String.class, "attemptId"),
	TASK_STATUS(String.class, "taskStatus"),
	COPY_PHASE(String.class, null),
	SORT_PHASE(String.class, null),
	REDUCE_PHASE(String.class, null),
	SHUFFLE_FINISHED(Long.class, "shuffleFinishTime"),
	SORT_FINISHED(Long.class, "sortFinishTime"),
	COUNTERS(String.class, null),
	SPLITS(String.class, "splitLocations"),
	JOB_PRIORITY(String.class, null),
	HTTP_PORT(Integer.class, "httpPort"),
	TRACKER_NAME(String.class, "trackerName"),
	STATE_STRING(String.class, "state"),
	VERSION(String.class, null),
	MAP_COUNTERS(String.class, null),
	REDUCE_COUNTERS(String.class, null),
	VIEW_JOB(String.class, null),
	MODIFY_JOB(String.class, null),
	JOB_QUEUE(String.class, "jobQueueName"),
	// hadoop 2.0 related keys {@link JobHistoryParser}
  applicationAttemptId(String.class, null),
  containerId(String.class, null),
  successfulAttemptId(String.class, null),
  failedDueToAttempt(String.class, null),
  workflowId(String.class, null),
  workflowName(String.class, null),
  workflowNodeName(String.class, null),
  workflowAdjacencies(String.class, null),
  locality(String.class, null),
  avataar(String.class, null),
  nodeManagerHost(String.class, null),
  nodeManagerPort(Integer.class, null),
  nodeManagerHttpPort(Integer.class, null),
  acls(String.class, null),
  uberized(String.class, null),
  shufflePort(Integer.class, null),
  mapFinishTime(Long.class, null),
  port(Integer.class, null),
  rackname(String.class, null),
  clockSplits(String.class, null),
  cpuUsages(String.class, null),
  physMemKbytes(String.class, null),
  vMemKbytes(String.class, null),
  status(String.class, null),
  TOTAL_COUNTERS(String.class, null),
  TASK_COUNTERS(String.class, null),
  TASK_ATTEMPT_COUNTERS(String.class, null);

  private final Class<?> className;
  private final String mapping;

  private JobHistoryKeys(Class<?> className, String mapping) {
    this.className = className;
    this.mapping = mapping;
    }

  public Class<?> getClassName() {
    return className;
  }

  /**
   * Data types represented by each of the defined job history field names
   */
  public static Map<JobHistoryKeys, Class<?>> KEY_TYPES = Maps.newHashMap();
  static {
    for (JobHistoryKeys t : JobHistoryKeys.values()) {
      KEY_TYPES.put(t, t.getClassName());
    }
  }

  /**
   * Mapping the keys in 2.0 job history files to 1.0 key names
   */
  public static Map<String, String> HADOOP2_TO_HADOOP1_MAPPING = Maps.newHashMap();
  static {
    for (JobHistoryKeys t : JobHistoryKeys.values()) {
      if (StringUtils.isNotEmpty(t.mapping)) {
        HADOOP2_TO_HADOOP1_MAPPING.put(t.mapping,t.toString());
      }
    }
  }


	/**
	 * Job history key names as bytes
	 */
  public static final Map<JobHistoryKeys, byte[]> KEYS_TO_BYTES =
      new HashMap<JobHistoryKeys, byte[]>();
	static {
	  for (JobHistoryKeys k : JobHistoryKeys.values()) {
	    KEYS_TO_BYTES.put(k, Bytes.toBytes(k.toString().toLowerCase()));
	  }
	}
}