package com.twitter.hraven;

import java.util.HashMap;
import java.util.Map;

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
	JOBTRACKERID(String.class),
	START_TIME(Long.class),
	FINISH_TIME(Long.class),
	JOBID(String.class),
	JOBNAME(String.class),
	USER(String.class),
	JOBCONF(String.class),
	SUBMIT_TIME(Long.class),
	LAUNCH_TIME(Long.class),
	TOTAL_MAPS(Long.class),
	TOTAL_REDUCES(Long.class),
	FAILED_MAPS(Long.class),
	FAILED_REDUCES(Long.class),
	FINISHED_MAPS(Long.class),
	FINISHED_REDUCES(Long.class),
	JOB_STATUS(String.class),
	TASKID(String.class),
	HOSTNAME(String.class),
	TASK_TYPE(String.class),
	ERROR(String.class),
	TASK_ATTEMPT_ID(String.class),
	TASK_STATUS(String.class),
	COPY_PHASE(String.class),
	SORT_PHASE(String.class),
	REDUCE_PHASE(String.class),
	SHUFFLE_FINISHED(Long.class),
	SORT_FINISHED(Long.class),
	COUNTERS(String.class),
	SPLITS(String.class),
	JOB_PRIORITY(String.class),
	HTTP_PORT(Integer.class),
	TRACKER_NAME(String.class),
	STATE_STRING(String.class),
	VERSION(String.class),
	MAP_COUNTERS(String.class),
	REDUCE_COUNTERS(String.class),
	VIEW_JOB(String.class),
	MODIFY_JOB(String.class),
	JOB_QUEUE(String.class),
	// hadoop 2.0 related keys {@link JobHistoryParser}
  applicationAttemptId(String.class),
  containerId(String.class),
  successfulAttemptId(String.class),
  workflowId(String.class),
  workflowName(String.class),
  workflowNodeName(String.class),
  workflowAdjacencies(String.class),
  locality(String.class),
  avataar(String.class),
  nodeManagerHost(String.class),
  nodeManagerPort(Integer.class),
  nodeManagerHttpPort(Integer.class),
  acls(String.class),
  uberized(String.class),
  shufflePort(Integer.class),
  mapFinishTime(Long.class),
  port(Integer.class),
  rackname(String.class),
  clockSplits(String.class),
  cpuUsages(String.class),
  physMemKbytes(String.class),
  vMemKbytes(String.class),
  status(String.class),
  TOTAL_COUNTERS(String.class),
  TASK_COUNTERS(String.class),
  TASK_ATTEMPT_COUNTERS(String.class);

  private final Class<?> className;

  private JobHistoryKeys(Class<?> className) {
    this.className = className;
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