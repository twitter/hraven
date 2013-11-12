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
package com.twitter.hraven.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.GenerateFlowTestData;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.datasource.HRavenTestUtil;

/**
 * Round-trip testing for storage and retrieval of data in job_history table.
 *
 */
public class TestJobHistoryService {
  private static Log LOG = LogFactory.getLog(TestJobHistoryService.class);
  private static HBaseTestingUtility UTIL;
  private static HTable historyTable;
  private static JobHistoryByIdService idService;
  private static GenerateFlowTestData flowDataGen ;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);
    historyTable = new HTable(UTIL.getConfiguration(), Constants.HISTORY_TABLE_BYTES);
    idService = new JobHistoryByIdService(UTIL.getConfiguration());
    flowDataGen = new GenerateFlowTestData();

  }

  @Test
  public void testJobHistoryRead() throws Exception {
    // load some initial data
    // a few runs of the same app
	 
    flowDataGen.loadFlow("c1@local", "buser", "app1", 1234, "a", 3, 10,idService, historyTable);
    flowDataGen.loadFlow("c1@local", "buser", "app1", 1345, "a", 3, 10,idService, historyTable);
    flowDataGen.loadFlow("c1@local", "buser", "app1", 1456, "a", 3, 10,idService, historyTable);

    flowDataGen.loadFlow("c1@local", "buser", "app2", 1212, "a", 1, 10,idService, historyTable);

    flowDataGen.loadFlow("c1@local", "fuser", "app1", 2345, "a", 2, 10,idService, historyTable);
    flowDataGen.loadFlow("c1@local", "fuser", "app1", 2456, "b", 2, 10,idService, historyTable);

    // read out job history flow directly
    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    try {
      Flow flow = service.getLatestFlow("c1@local", "buser", "app1");
      assertNotNull(flow);
      assertEquals(3, flow.getJobs().size());
      for (JobDetails j : flow.getJobs()) {
        JobKey k = j.getJobKey();
        assertEquals("c1@local", k.getCluster());
        assertEquals("buser", k.getUserName());
        assertEquals("app1", k.getAppId());
        assertEquals(1456L, k.getRunId());
        assertEquals("a", j.getVersion());
      }

      List<Flow> flowSeries = service.getFlowSeries("c1@local", "buser", "app1", 100);
      assertNotNull(flowSeries);
      assertEquals(3, flowSeries.size());
      for (Flow f : flowSeries) {
        for (JobDetails j : f.getJobs()) {
          JobKey k = j.getJobKey();
          assertEquals(f.getCluster(), k.getCluster());
          assertEquals(f.getUserName(), k.getUserName());
          assertEquals(f.getAppId(), k.getAppId());
          assertEquals(f.getRunId(), k.getRunId());
        }
      }

      flowSeries = service.getFlowSeries("c1@local", "buser", "app2", 100);
      assertNotNull(flowSeries);
      assertEquals(1, flowSeries.size());
      Flow first = flowSeries.get(0);
      assertEquals(1, first.getJobs().size());
      JobDetails firstJob = first.getJobs().get(0);
      assertEquals("c1@local", firstJob.getJobKey().getCluster());
      assertEquals("buser", firstJob.getJobKey().getUserName());
      assertEquals("app2", firstJob.getJobKey().getAppId());
      assertEquals(1212L, firstJob.getJobKey().getRunId());

      flowSeries = service.getFlowSeries("c1@local", "fuser", "app1", 100);
      assertNotNull(flowSeries);
      assertEquals(2, flowSeries.size());
      Flow f1 = flowSeries.get(0);
      assertEquals(2, f1.getJobs().size());
      assertEquals("fuser", f1.getUserName());
      assertEquals("app1", f1.getAppId());
      for (JobDetails j : f1.getJobs()) {
        assertEquals(2456L, j.getJobKey().getRunId());
        assertEquals("b", j.getVersion());
      }
      Flow f2 = flowSeries.get(1);
      assertEquals(2, f2.getJobs().size());
      assertEquals("fuser", f2.getUserName());
      assertEquals("app1", f2.getAppId());
      for (JobDetails j : f2.getJobs()) {
        assertEquals(2345L, j.getJobKey().getRunId());
        assertEquals("a", j.getVersion());
      }

      // test reading job history flow by job ID
      String jobId = f2.getJobs().get(0).getJobId();
      Flow f2FromId = service.getFlowByJobID("c1@local", jobId, false);
      assertNotNull(f2FromId);
      assertEquals(f2.getCluster(), f2FromId.getCluster());
      assertEquals(f2.getUserName(), f2FromId.getUserName());
      assertEquals(f2.getAppId(), f2FromId.getAppId());
      assertEquals(f2.getRunId(), f2FromId.getRunId());
      assertEquals(f2.getJobs().size(), f2FromId.getJobs().size());
      for (int i=0; i<f2.getJobs().size(); i++) {
        JobDetails j1 = f2.getJobs().get(i);
        JobDetails j2 = f2FromId.getJobs().get(i);
        assertEquals(j1.getJobKey(), j2.getJobKey());
        assertEquals(j1.getVersion(), j2.getVersion());
      }

      // try reading a flow series limited to a specific version
      List<Flow> versionSeries = service.getFlowSeries("c1@local", "fuser", "app1", "a", false, 100);
      assertNotNull(versionSeries);
      assertEquals(1, versionSeries.size());
      for (JobDetails j : versionSeries.get(0).getJobs()) {
        assertEquals(2345L, j.getJobKey().getRunId());
        assertEquals("a", j.getVersion());
      }
    } finally {
      service.close();
    }
  }

  @Test
  public void testGetJobByJobID() throws Exception {
    // load a sample flow
    flowDataGen.loadFlow("c1@local", "buser", "getJobByJobID", 1234, "a", 3, 10,
        idService, historyTable);

    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    try {
      // fetch back the entire flow
      Flow flow = service.getLatestFlow("c1@local", "buser", "getJobByJobID");
      assertNotNull(flow);
      assertEquals(3, flow.getJobs().size());
      // for each job in the flow, validate that we can retrieve it individually
      for (JobDetails j : flow.getJobs()) {
        JobKey key = j.getJobKey();
        JobDetails j2 = service.getJobByJobID(key.getQualifiedJobId(), false);
        assertJob(j, j2);
      }
    } finally {
      service.close();
    }
  }

  @Test
  public void testGetFlowTimeSeriesStats() throws Exception {

    // load a sample flow
    final short numJobsAppOne = 3 ;
    final short numJobsAppTwo = 4 ;
    final long baseStats = 10L ;

    flowDataGen.loadFlow("c1@local", "buser", "AppOne", 1234, "a", numJobsAppOne, baseStats,
        idService, historyTable);
    flowDataGen.loadFlow("c1@local", "buser", "AppTwo", 2345, "b", numJobsAppTwo, baseStats,
        idService, historyTable);

    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    try {
      // fetch back the entire flow stats
      List<Flow> flowSeries = service.getFlowTimeSeriesStats("c1@local", "buser", "AppOne", "", 0L, 0L, 1000, null);
      assertNotNull(flowSeries);
      for ( Flow f : flowSeries ){
        assertEquals( numJobsAppOne, f.getJobCount());
        assertEquals( numJobsAppOne * baseStats , f.getTotalMaps());
        assertEquals( numJobsAppOne * baseStats , f.getTotalReduces());
        assertEquals( numJobsAppOne * baseStats , f.getHdfsBytesRead());
        assertEquals( numJobsAppOne * baseStats , f.getHdfsBytesWritten());
        assertEquals( numJobsAppOne * baseStats , f.getMapFileBytesRead());
        assertEquals( numJobsAppOne * baseStats , f.getMapFileBytesWritten());
        assertEquals( numJobsAppOne * baseStats , f.getMapSlotMillis());
        assertEquals( numJobsAppOne * baseStats , f.getReduceFileBytesRead());
        assertEquals( numJobsAppOne * baseStats , f.getReduceShuffleBytes());
        assertEquals( numJobsAppOne * baseStats , f.getReduceSlotMillis());
        assertEquals( "a" , f.getVersion());
        assertEquals( numJobsAppOne * 1000, f.getDuration());
        // verify that job configurations are empty
        for (JobDetails job : f.getJobs()) {
          assertEquals(0, job.getConfiguration().size());
        }
      }

      flowSeries = service.getFlowTimeSeriesStats("c1@local", "buser", "AppTwo", "", 0L, 0L, 1000, null);
      assertNotNull(flowSeries);
      for ( Flow f : flowSeries ){
        assertEquals( numJobsAppTwo, f.getJobCount());
        assertEquals( numJobsAppTwo * baseStats , f.getTotalMaps());
        assertEquals( numJobsAppTwo * baseStats , f.getTotalReduces());
        assertEquals( numJobsAppTwo * baseStats , f.getHdfsBytesRead());
        assertEquals( numJobsAppTwo * baseStats , f.getHdfsBytesWritten());
        assertEquals( numJobsAppTwo * baseStats , f.getMapFileBytesRead());
        assertEquals( numJobsAppTwo * baseStats , f.getMapFileBytesWritten());
        assertEquals( numJobsAppTwo * baseStats , f.getMapSlotMillis());
        assertEquals( numJobsAppTwo * baseStats , f.getReduceFileBytesRead());
        assertEquals( numJobsAppTwo * baseStats , f.getReduceShuffleBytes());
        assertEquals( numJobsAppTwo * baseStats , f.getReduceSlotMillis());
        assertEquals( "b" , f.getVersion());
        assertEquals( numJobsAppTwo * 1000, f.getDuration());
      }
    } finally {
      service.close();
    }
  }

  @Test
  public void testRemoveJob() throws Exception {
    // load a sample flow
    flowDataGen.loadFlow("c1@local", "ruser", "removeJob", 1234, "a", 3, 10,idService, historyTable);

    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    try {
      // fetch back the entire flow
      Flow flow = service.getLatestFlow("c1@local", "ruser", "removeJob");
      assertNotNull(flow);
      assertEquals(3, flow.getJobs().size());

      // remove the first job
      List<JobDetails> origJobs = flow.getJobs();
      JobDetails toRemove = origJobs.get(0);
      // drop the the collection so we can compare remaining
      origJobs.remove(0);
      LOG.info("Removing job "+toRemove.getJobKey());
      service.removeJob(toRemove.getJobKey());

      Flow flow2 = service.getLatestFlow("c1@local", "ruser", "removeJob");
      assertNotNull(flow2);
      assertEquals(2, flow2.getJobs().size());
      for (JobDetails j : flow2.getJobs()) {
        if (j.getJobKey().equals(toRemove.getJobKey())) {
          fail("Removed job ("+toRemove.getJobKey()+") is still present in flow!");
        }
      }

      // remaining jobs in the flow should match
      List<JobDetails> flow2Jobs = flow2.getJobs();
      assertEquals(origJobs.size(), flow2Jobs.size());
      for (int i=0; i < origJobs.size(); i++) {
        JobDetails j1 = origJobs.get(i);
        JobDetails j2 = flow2Jobs.get(i);
        assertJob(j1, j2);
      }
      // TODO: validate deletion of task rows
    } finally {
      service.close();
    }
  }

  private void assertFoundOnce(byte[] column, Put jobPut, int expectedSize,
		  String expectedValue) {
	  boolean foundUserName = false;
	  List<KeyValue> kv1 = jobPut.get(Constants.INFO_FAM_BYTES, column);
	  assertEquals(expectedSize, kv1.size());
	  for (KeyValue kv : kv1) {
		assertEquals(Bytes.toString(kv.getValue()), expectedValue);
	    // ensure we don't see the same put twice
		assertFalse(foundUserName);
		// now set this to true
		foundUserName = true;
  	  }
      // ensure that we got the user name
	  assertTrue(foundUserName);
  }

  @Test
  public void testSetHravenQueueName() throws FileNotFoundException {

	  final String JOB_CONF_FILE_NAME =
		        "src/test/resources/job_1329348432655_0001_conf.xml";

	  Configuration jobConf = new Configuration();
	  jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));

	  String USERNAME = "user";
	  JobKey jobKey = new JobKey("cluster1", USERNAME, "Sleep", 1,
			  "job_1329348432655_0001");
	  byte[] jobKeyBytes = new JobKeyConverter().toBytes(jobKey);
	  Put jobPut = new Put(jobKeyBytes);
	  byte[] jobConfColumnPrefix = Bytes.toBytes(Constants.JOB_CONF_COLUMN_PREFIX
	        + Constants.SEP);

	  assertEquals(jobPut.size(), 0);

	  // check queuename matches user name since the conf has
	  // value "default" as the queuename
	  JobHistoryService.setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
	  assertEquals(jobPut.size(), 1);
	  byte[] column = Bytes.add(jobConfColumnPrefix, Constants.HRAVEN_QUEUE_BYTES);
	  assertFoundOnce(column, jobPut, 1, USERNAME);

	  // populate the jobConf with all types of queue name parameters
	  String expH2QName = "hadoop2queue";
	  String expH1PoolName = "fairpool";
	  String capacityH1QName = "capacity1aueue";
	  jobConf.set(Constants.QUEUENAME_HADOOP2, expH2QName);
	  jobConf.set(Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1, expH1PoolName);
	  jobConf.set(Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1, capacityH1QName);

	  // now check queuename is correctly set as hadoop2 queue name
	  // even when the fairscheduler and capacity scheduler are set
	  jobPut = new Put(jobKeyBytes);
	  assertEquals(jobPut.size(), 0);
	  JobHistoryService.setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
	  assertEquals(jobPut.size(), 1);
	  assertFoundOnce(column, jobPut, 1, expH2QName);

	  // now unset hadoop2 queuename, expect fairscheduler name to be used as queuename
	  jobConf.set(Constants.QUEUENAME_HADOOP2, "");
	  jobPut = new Put(jobKeyBytes);
	  assertEquals(jobPut.size(), 0);
	  JobHistoryService.setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
	  assertEquals(jobPut.size(), 1);
	  assertFoundOnce(column, jobPut, 1, expH1PoolName);

	  // now unset fairscheduler name, expect capacity scheduler to be used as queuename
	  jobConf.set(Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1, "");
	  jobPut = new Put(jobKeyBytes);
	  assertEquals(jobPut.size(), 0);
	  JobHistoryService.setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
	  assertEquals(jobPut.size(), 1);
	  assertFoundOnce(column, jobPut, 1, capacityH1QName);

	  // now unset capacity scheduler, expect default_queue to be used as queuename
	  jobConf.set(Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1, "");
	  jobPut = new Put(jobKeyBytes);
	  assertEquals(jobPut.size(), 0);
	  JobHistoryService.setHravenQueueNamePut(jobConf, jobPut, jobKey, jobConfColumnPrefix);
	  assertEquals(jobPut.size(), 1);
	  assertFoundOnce(column, jobPut, 1, Constants.DEFAULT_QUEUENAME);

  }

  private void assertJob(JobDetails expected, JobDetails actual) {
    assertNotNull(actual);
    assertEquals(expected.getJobKey(), actual.getJobKey());
    assertEquals(expected.getJobId(), actual.getJobId());
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getVersion(), actual.getVersion());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
