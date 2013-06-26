package com.twitter.hraven;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

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
	JOBTRACKERID, START_TIME, FINISH_TIME, 
	JOBID, JOBNAME, USER, JOBCONF, SUBMIT_TIME, 
	LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES, 
	FAILED_MAPS, FAILED_REDUCES, 
	FINISHED_MAPS, FINISHED_REDUCES, 
	JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE, 
	ERROR, TASK_ATTEMPT_ID, TASK_STATUS, 
	COPY_PHASE, SORT_PHASE, REDUCE_PHASE, 
	SHUFFLE_FINISHED, SORT_FINISHED, COUNTERS,
	SPLITS, JOB_PRIORITY, HTTP_PORT, 
	TRACKER_NAME, STATE_STRING, VERSION, 
	MAP_COUNTERS, REDUCE_COUNTERS, 
	VIEW_JOB, MODIFY_JOB, JOB_QUEUE;

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

	/**
	 * Data types represented by each of the defined job history field names
	 */
	@SuppressWarnings("rawtypes")
	public static Map<JobHistoryKeys, Class> KEY_TYPES = new HashMap<JobHistoryKeys, Class>();
	static {
		KEY_TYPES.put(JOBTRACKERID, String.class);
		KEY_TYPES.put(START_TIME, Long.class);
		KEY_TYPES.put(FINISH_TIME, Long.class);
		KEY_TYPES.put(JOBID, String.class);
		KEY_TYPES.put(JOBNAME, String.class);
		KEY_TYPES.put(USER, String.class);
		KEY_TYPES.put(JOBCONF, String.class);
		KEY_TYPES.put(SUBMIT_TIME, Long.class);
		KEY_TYPES.put(LAUNCH_TIME, Long.class);
		KEY_TYPES.put(TOTAL_MAPS, Long.class);
		KEY_TYPES.put(TOTAL_REDUCES, Long.class);
		KEY_TYPES.put(FAILED_MAPS, Long.class);
		KEY_TYPES.put(FAILED_REDUCES, Long.class);
		KEY_TYPES.put(FINISHED_MAPS, Long.class);
		KEY_TYPES.put(FINISHED_REDUCES, Long.class);
		KEY_TYPES.put(JOB_STATUS, String.class);
		KEY_TYPES.put(TASKID, String.class);
		KEY_TYPES.put(HOSTNAME, String.class);
		KEY_TYPES.put(TASK_TYPE, String.class);
		KEY_TYPES.put(ERROR, String.class);
		KEY_TYPES.put(TASK_ATTEMPT_ID, String.class);
		KEY_TYPES.put(TASK_STATUS, String.class);
		KEY_TYPES.put(COPY_PHASE, String.class);
		KEY_TYPES.put(SORT_PHASE, String.class);
		KEY_TYPES.put(REDUCE_PHASE, String.class);
		KEY_TYPES.put(SHUFFLE_FINISHED, Long.class);
		KEY_TYPES.put(SORT_FINISHED, Long.class);
		KEY_TYPES.put(COUNTERS, String.class);
		KEY_TYPES.put(SPLITS, String.class);
		KEY_TYPES.put(JOB_PRIORITY, String.class);
		KEY_TYPES.put(HTTP_PORT, Integer.class);
		KEY_TYPES.put(TRACKER_NAME, String.class);
		KEY_TYPES.put(STATE_STRING, String.class);
		KEY_TYPES.put(VERSION, String.class);
		KEY_TYPES.put(MAP_COUNTERS, String.class);
		KEY_TYPES.put(REDUCE_COUNTERS, String.class);
		KEY_TYPES.put(VIEW_JOB, String.class);
		KEY_TYPES.put(MODIFY_JOB, String.class);
		KEY_TYPES.put(JOB_QUEUE, String.class);
	}

}