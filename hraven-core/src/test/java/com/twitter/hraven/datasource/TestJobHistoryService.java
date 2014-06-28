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
import com.twitter.hraven.HadoopVersion;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
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

    // load flows for checking timebound flow scan with version
    flowDataGen.loadFlow("c3@local", "kuser", "app9", 1395786712000L,
        "version", 2, 10,idService, historyTable);
    flowDataGen.loadFlow("c3@local", "kuser", "app9", 1395786725000L,
      "version", 2, 10,idService, historyTable);
    flowDataGen.loadFlow("c3@local", "kuser", "app9", 1395786712000L,
      "version2", 3, 10,idService, historyTable);

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

      // check the timebound scan for default time
      long endTime = System.currentTimeMillis();
      long startTime = endTime - Constants.THIRTY_DAYS_MILLIS;
      flowSeries = service.getFlowSeries("c1@local", "buser", "app2",
        "a", true, startTime, endTime, 100);
      assertNotNull(flowSeries);
      assertEquals(0, flowSeries.size());

      // check the timebound scan for start and end times
      endTime = System.currentTimeMillis();
      startTime = 1395786712000L - 86400000L;
      flowSeries = service.getFlowSeries( "c3@local", "kuser", "app9",
        "version",true, startTime, endTime, 100);
      assertNotNull(flowSeries);
      assertEquals(2, flowSeries.size());

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

  @SuppressWarnings("deprecation")
  private void checkSomeFlowStats(String version, HadoopVersion hv, int numJobs, long baseStats, List<Flow> flowSeries) {
    assertNotNull(flowSeries);
    for ( Flow f : flowSeries ){
      assertEquals( numJobs, f.getJobCount());
      assertEquals( numJobs * baseStats , f.getTotalMaps());
      assertEquals( numJobs * baseStats , f.getTotalReduces());
      assertEquals( numJobs * baseStats , f.getHdfsBytesRead());
      assertEquals( numJobs * baseStats , f.getHdfsBytesWritten());
      assertEquals( numJobs * baseStats , f.getMapFileBytesRead());
      assertEquals( numJobs * baseStats , f.getMapFileBytesWritten());
      assertEquals( numJobs * baseStats , f.getMapSlotMillis());
      assertEquals( numJobs * baseStats , f.getReduceFileBytesRead());
      assertEquals( numJobs * baseStats , f.getReduceShuffleBytes());
      assertEquals( numJobs * baseStats , f.getReduceSlotMillis());
      assertEquals( version , f.getVersion());
      assertEquals( hv, f.getHadoopVersion());
      assertEquals( numJobs * baseStats , f.getMegabyteMillis());
      assertEquals( numJobs * 1000, f.getDuration());
      assertEquals( f.getDuration() + GenerateFlowTestData.SUBMIT_LAUCH_DIFF, f.getWallClockTime());
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
      checkSomeFlowStats("a", HadoopVersion.ONE, numJobsAppOne, baseStats, flowSeries);

      flowSeries = service.getFlowTimeSeriesStats("c1@local", "buser", "AppTwo", "", 0L, 0L, 1000, null);
      checkSomeFlowStats("b", HadoopVersion.ONE, numJobsAppTwo, baseStats, flowSeries);

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

  @Test
  public void testSetHravenQueueName() throws FileNotFoundException {

	  final String JOB_CONF_FILE_NAME =
		        "src/test/resources/job_1329348432655_0001_conf.xml";

	  Configuration jobConf = new Configuration();
	  jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));

	  String USERNAME = "user";
	  JobKey jobKey = new JobKey("cluster1", USERNAME, "Sleep", 1,
			  "job_1329348432655_0001");
	  
	  JobHistoryRecordCollection recordCollection = new JobHistoryRecordCollection(jobKey);

	  assertEquals(recordCollection.size(), 0);

	  // check queuename matches user name since the conf has
	  // value "default" as the queuename
	  JobHistoryService.setHravenQueueNameRecord(jobConf, recordCollection, jobKey);
	  assertEquals(recordCollection.size(), 1);
	  assertEquals(recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE)), USERNAME);

	  // populate the jobConf with all types of queue name parameters
	  String expH2QName = "hadoop2queue";
	  String expH1PoolName = "fairpool";
	  String capacityH1QName = "capacity1aueue";
	  jobConf.set(Constants.QUEUENAME_HADOOP2, expH2QName);
	  jobConf.set(Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1, expH1PoolName);
	  jobConf.set(Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1, capacityH1QName);

	  // now check queuename is correctly set as hadoop2 queue name
	  // even when the fairscheduler and capacity scheduler are set
	  recordCollection = new JobHistoryRecordCollection(jobKey);
	  assertEquals(recordCollection.size(), 0);
	  JobHistoryService.setHravenQueueNameRecord(jobConf, recordCollection, jobKey);
	  assertEquals(recordCollection.size(), 1);
	  assertEquals(recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE)), expH2QName);

	  // now unset hadoop2 queuename, expect fairscheduler name to be used as queuename
	  jobConf.set(Constants.QUEUENAME_HADOOP2, "");
	  recordCollection = new JobHistoryRecordCollection(jobKey);
	  assertEquals(recordCollection.size(), 0);
	  JobHistoryService.setHravenQueueNameRecord(jobConf, recordCollection, jobKey);
	  assertEquals(recordCollection.size(), 1);
	  assertEquals(recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE)), expH1PoolName);

	  // now unset fairscheduler name, expect capacity scheduler to be used as queuename
	  jobConf.set(Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1, "");
	  recordCollection = new JobHistoryRecordCollection(jobKey);
	  assertEquals(recordCollection.size(), 0);
	  JobHistoryService.setHravenQueueNameRecord(jobConf, recordCollection, jobKey);
	  assertEquals(recordCollection.size(), 1);
	  assertEquals(recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE)), capacityH1QName);

	  // now unset capacity scheduler, expect default_queue to be used as queuename
	  jobConf.set(Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1, "");
	  recordCollection = new JobHistoryRecordCollection(jobKey);
	  assertEquals(recordCollection.size(), 0);
	  JobHistoryService.setHravenQueueNameRecord(jobConf, recordCollection, jobKey);
	  assertEquals(recordCollection.size(), 1);
	  assertEquals(recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE)), Constants.DEFAULT_QUEUENAME);
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
