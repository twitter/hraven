package com.twitter.hraven;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashSet;
import org.junit.Test;

public class TestJobHistoryKeys {
  
  private enum test_keys {
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
      VIEW_JOB, MODIFY_JOB, JOB_QUEUE,
      // hadoop 2.0 related keys {@link JobHistoryParser}
      applicationAttemptId, containerId, nodeManagerHost,
      successfulAttemptId, failedDueToAttempt,
      workflowId, workflowName, workflowNodeName,
      workflowAdjacencies, locality, avataar,
      nodeManagerPort, nodeManagerHttpPort,
      acls, uberized, shufflePort, mapFinishTime,
      port, rackname, clockSplits, cpuUsages,
      physMemKbytes, vMemKbytes, status, TOTAL_COUNTERS,
      TASK_COUNTERS, TASK_ATTEMPT_COUNTERS;
  }
  
  @Test
  public void test_contents() {
  
    HashSet<String> tk_values = new HashSet<String>();
    for (test_keys tk : test_keys.values()) {
      tk_values.add(tk.name());
    }
    
    for (JobHistoryKeys jhk : JobHistoryKeys.values()) {
      assertTrue(tk_values.contains(jhk.name()));
    }    
  }
  
  @Test
  public void test_key_types() {
    
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOBTRACKERID), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.START_TIME), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.FINISH_TIME), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOBID), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOBNAME), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.USER), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOBCONF), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.SUBMIT_TIME), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.LAUNCH_TIME), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TOTAL_MAPS), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TOTAL_REDUCES), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.FAILED_MAPS), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.FAILED_REDUCES), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.FINISHED_MAPS), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.FINISHED_REDUCES), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOB_STATUS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TASKID), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.HOSTNAME), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TASK_TYPE), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.ERROR), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TASK_ATTEMPT_ID), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TASK_STATUS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.COPY_PHASE), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.SORT_PHASE), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.REDUCE_PHASE), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.SHUFFLE_FINISHED), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.SORT_FINISHED), Long.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.COUNTERS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.SPLITS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOB_PRIORITY), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.HTTP_PORT), Integer.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.TRACKER_NAME), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.STATE_STRING), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.VERSION), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.MAP_COUNTERS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.REDUCE_COUNTERS), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.VIEW_JOB), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.MODIFY_JOB), String.class);
    assertEquals(JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.JOB_QUEUE), String.class);
  }
  
}