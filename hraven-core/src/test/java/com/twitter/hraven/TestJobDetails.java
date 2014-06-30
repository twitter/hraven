/*
 * Copyright 2013 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven;

import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit tests for functions in the {@link JobDetails} class specifically for the populate function
 * to enable returning different counter sub group names that changed between hadoop 1 and hadoop 2
 */
public class TestJobDetails {

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  private static final class JobDetailsValues {
    // job-level stats
    static final String jobName = "Sleep Job";
    static final String jobId = "job_12345678910_1234";
    static final String user = "user1";
    static final String priority = "NORMAL";
    static final String status = "SUCCESS";
    static final String version = "5b6900cfdcaa2a17db3d5f3f";
    static final long totalMaps = 100L;
    static final long totalReduces = 15L;
    static final long finishedMaps = 99L;
    static final long finishedReduces = 15L;
    static final long failedMaps = 1L;
    static final long failedReduces = 0L;
    static final long submitTime = 1376617540985L;
    static final long launchTime = 1376617544263L;
    static final long finishTime = 1376618781318L;
    static final long megabytemillis = 46317568L;

    // hadoop 1 counter values
    static final long FILES_BYTES_READ_1 = 202401720425L;
    static final long FILES_BYTES_WRITTEN_1 = 213924145507L;
    static final long REDUCE_SHUFFLE_BYTES_1 = 106017635120L;
    static final long HDFS_BYTES_READ_1 = 374344244609L;
    static final long HDFS_BYTES_WRITTEN_1 = 122203940055L;
    static final long SLOTS_MILLIS_MAPS_1 = 209069728L;
    static final long SLOTS_MILLIS_REDUCES_1 = 110237644L;

    // hadoop 2 counter values
    static final long FILES_BYTES_READ_2 = 202401720425L;
    static final long FILES_BYTES_WRITTEN_2 = 213924145507L;
    static final long REDUCE_SHUFFLE_BYTES_2 = 106017635120L;
    static final long HDFS_BYTES_READ_2 = 374344244609L;
    static final long HDFS_BYTES_WRITTEN_2 = 122203940055L;
    static final long SLOTS_MILLIS_MAPS_2 = 209069728L;
    static final long SLOTS_MILLIS_REDUCES_2 = 110237644L;
  }

  private void addInMapCounters(NavigableMap<byte[], byte[]> infoValues) {

    // hadoop1 counters
    byte[] c1 = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    byte[] c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTERS.toString()));
    byte[] c3 = Bytes.add(c2, Constants.SEP_BYTES);

    byte[] c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_READ.toString()));

    byte[] value = Bytes.toBytes(JobDetailsValues.FILES_BYTES_READ_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_WRITTEN.toString()));
    infoValues.put(c4, Bytes.toBytes(JobDetailsValues.FILES_BYTES_WRITTEN_2));

    // hadoop2 counters
    c1 = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTER_HADOOP2.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_READ.toString()));

    value = Bytes.toBytes(JobDetailsValues.FILES_BYTES_READ_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_WRITTEN.toString()));
    infoValues.put(c4, Bytes.toBytes(JobDetailsValues.FILES_BYTES_WRITTEN_2));
  }

  private void addInReduceCounters(NavigableMap<byte[], byte[]> infoValues) {

    // hadoop 1 counters
    byte[] c1 = Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    byte[] c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTERS.toString()));
    byte[] c3 = Bytes.add(c2, Constants.SEP_BYTES);

    byte[] c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_READ.toString()));

    byte[] value = Bytes.toBytes(JobDetailsValues.FILES_BYTES_READ_2);
    infoValues.put(c4, value);

    c2 = Bytes.add(c1, Bytes.toBytes(Constants.TASK_COUNTER.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);
    c4 = Bytes.add(c3, Bytes.toBytes(Constants.REDUCE_SHUFFLE_BYTES.toString()));
    infoValues.put(c4, Bytes.toBytes(JobDetailsValues.REDUCE_SHUFFLE_BYTES_2));

    // hadoop2 counters
    c1 = Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTER_HADOOP2.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.FILES_BYTES_READ.toString()));

    value = Bytes.toBytes(JobDetailsValues.FILES_BYTES_READ_2);
    infoValues.put(c4, value);

    c2 = Bytes.add(c1, Bytes.toBytes(Constants.TASK_COUNTER_HADOOP2.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);
    c4 = Bytes.add(c3, Bytes.toBytes(Constants.REDUCE_SHUFFLE_BYTES.toString()));
    infoValues.put(c4, Bytes.toBytes(JobDetailsValues.REDUCE_SHUFFLE_BYTES_2));

  }

  private void addInTotalCounters(NavigableMap<byte[], byte[]> infoValues) {

    // hadoop 1 counters
    byte[] c1 = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    byte[] c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTERS.toString()));
    byte[] c3 = Bytes.add(c2, Constants.SEP_BYTES);

    byte[] c4 = Bytes.add(c3, Bytes.toBytes(Constants.HDFS_BYTES_READ.toString()));
    byte[] value = Bytes.toBytes(JobDetailsValues.HDFS_BYTES_READ_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.HDFS_BYTES_WRITTEN.toString()));
    value = Bytes.toBytes(JobDetailsValues.HDFS_BYTES_WRITTEN_2);
    infoValues.put(c4, value);

    c2 = Bytes.add(c1, Bytes.toBytes(Constants.JOBINPROGRESS_COUNTER.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);
    c4 = Bytes.add(c3, Bytes.toBytes(Constants.SLOTS_MILLIS_MAPS.toString()));
    value = Bytes.toBytes(JobDetailsValues.SLOTS_MILLIS_MAPS_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.SLOTS_MILLIS_REDUCES.toString()));
    value = Bytes.toBytes(JobDetailsValues.SLOTS_MILLIS_REDUCES_2);
    infoValues.put(c4, value);

    // hadoop 2 counters
    c1 = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
    c2 = Bytes.add(c1, Bytes.toBytes(Constants.FILESYSTEM_COUNTER_HADOOP2.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.HDFS_BYTES_READ.toString()));
    value = Bytes.toBytes(JobDetailsValues.HDFS_BYTES_READ_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.HDFS_BYTES_WRITTEN.toString()));
    value = Bytes.toBytes(JobDetailsValues.HDFS_BYTES_WRITTEN_2);
    infoValues.put(c4, value);

    c2 = Bytes.add(c1, Bytes.toBytes(Constants.JOB_COUNTER_HADOOP2.toString()));
    c3 = Bytes.add(c2, Constants.SEP_BYTES);
    c4 = Bytes.add(c3, Bytes.toBytes(Constants.SLOTS_MILLIS_MAPS.toString()));
    value = Bytes.toBytes(JobDetailsValues.SLOTS_MILLIS_MAPS_2);
    infoValues.put(c4, value);

    c4 = Bytes.add(c3, Bytes.toBytes(Constants.SLOTS_MILLIS_REDUCES.toString()));
    value = Bytes.toBytes(JobDetailsValues.SLOTS_MILLIS_REDUCES_2);
    infoValues.put(c4, value);
  }

  private NavigableMap<byte[], byte[]> getInfoValues(HadoopVersion hv) {
    NavigableMap<byte[], byte[]> infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBID),
      Bytes.toBytes(JobDetailsValues.jobId));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.USER),
      Bytes.toBytes(JobDetailsValues.user));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBNAME),
      Bytes.toBytes(JobDetailsValues.jobName));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_PRIORITY),
      Bytes.toBytes(JobDetailsValues.priority));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_STATUS),
      Bytes.toBytes(JobDetailsValues.status));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.hadoopversion),
      Bytes.toBytes(hv.toString()));
    infoValues.put(Constants.VERSION_COLUMN_BYTES, Bytes.toBytes(JobDetailsValues.version));

    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.SUBMIT_TIME),
      Bytes.toBytes(JobDetailsValues.submitTime));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.LAUNCH_TIME),
      Bytes.toBytes(JobDetailsValues.launchTime));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISH_TIME),
      Bytes.toBytes(JobDetailsValues.finishTime));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_MAPS),
      Bytes.toBytes(JobDetailsValues.totalMaps));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_REDUCES),
      Bytes.toBytes(JobDetailsValues.totalReduces));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_MAPS),
      Bytes.toBytes(JobDetailsValues.finishedMaps));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_REDUCES),
      Bytes.toBytes(JobDetailsValues.finishedReduces));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_MAPS),
      Bytes.toBytes(JobDetailsValues.failedMaps));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_REDUCES),
      Bytes.toBytes(JobDetailsValues.failedReduces));
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_MAPS),
      Bytes.toBytes(JobDetailsValues.finishedMaps));
    infoValues.put(Constants.MEGABYTEMILLIS_BYTES,
      Bytes.toBytes(JobDetailsValues.megabytemillis));

    addInMapCounters(infoValues);
    addInReduceCounters(infoValues);
    addInTotalCounters(infoValues);

    return infoValues;
  }

  private void confirmSomeJobDeets(JobDetails jd) {
    assertEquals(JobDetailsValues.user, jd.getUser());
    assertEquals(JobDetailsValues.jobId, jd.getJobId());
    assertEquals(JobDetailsValues.jobName, jd.getJobName());
    assertEquals(JobDetailsValues.status, jd.getStatus());
    assertEquals(JobDetailsValues.version, jd.getVersion());
    assertEquals(JobDetailsValues.finishTime, jd.getFinishTime());
    assertEquals(JobDetailsValues.launchTime, jd.getLaunchTime());
    assertEquals(JobDetailsValues.submitTime, jd.getSubmitTime());
    assertEquals(JobDetailsValues.finishedMaps, jd.getFinishedMaps());
    assertEquals(JobDetailsValues.totalMaps, jd.getTotalMaps());
    assertEquals(JobDetailsValues.failedMaps, jd.getFailedMaps());
    assertEquals(JobDetailsValues.finishedReduces, jd.getFinishedReduces());
    assertEquals(JobDetailsValues.totalReduces, jd.getTotalReduces());
    assertEquals(JobDetailsValues.failedReduces, jd.getFailedReduces());
    assertEquals(JobDetailsValues.megabytemillis, jd.getMegabyteMillis());
  }

  private void confirmHadoop2Counters(JobDetails jd) {
    assertEquals(
      JobDetailsValues.FILES_BYTES_READ_2,
      jd.getMapCounters()
          .getCounter(Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_READ).getValue());

    assertEquals(
      JobDetailsValues.FILES_BYTES_WRITTEN_2,
      jd.getMapCounters()
          .getCounter(Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_WRITTEN)
          .getValue());

    assertEquals(
      JobDetailsValues.FILES_BYTES_READ_2,
      jd.getReduceCounters()
          .getCounter(Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_READ).getValue());

    assertEquals(
      JobDetailsValues.REDUCE_SHUFFLE_BYTES_2,
      jd.getReduceCounters()
          .getCounter(Constants.TASK_COUNTER_HADOOP2, Constants.REDUCE_SHUFFLE_BYTES).getValue());

    assertEquals(JobDetailsValues.HDFS_BYTES_READ_2,
      jd.getCounters().getCounter(Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.HDFS_BYTES_READ)
          .getValue());

    assertEquals(
      JobDetailsValues.HDFS_BYTES_WRITTEN_2,
      jd.getCounters()
          .getCounter(Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.HDFS_BYTES_WRITTEN)
          .getValue());

    assertEquals(JobDetailsValues.SLOTS_MILLIS_MAPS_2,
      jd.getCounters().getCounter(Constants.JOB_COUNTER_HADOOP2, Constants.SLOTS_MILLIS_MAPS)
          .getValue());

    assertEquals(JobDetailsValues.SLOTS_MILLIS_REDUCES_2,
      jd.getCounters().getCounter(Constants.JOB_COUNTER_HADOOP2, Constants.SLOTS_MILLIS_REDUCES)
          .getValue());
  }

  private void confirmHadoop1Counters(JobDetails jd) {
    assertEquals(JobDetailsValues.FILES_BYTES_READ_1,
      jd.getMapCounters().getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ)
          .getValue());

    assertEquals(JobDetailsValues.FILES_BYTES_WRITTEN_1,
      jd.getMapCounters().getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_WRITTEN)
          .getValue());

    assertEquals(JobDetailsValues.FILES_BYTES_READ_1,
      jd.getReduceCounters().getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ)
          .getValue());

    assertEquals(JobDetailsValues.REDUCE_SHUFFLE_BYTES_1,
      jd.getReduceCounters().getCounter(Constants.TASK_COUNTER, Constants.REDUCE_SHUFFLE_BYTES)
          .getValue());

    assertEquals(JobDetailsValues.HDFS_BYTES_READ_1,
      jd.getCounters().getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_READ)
          .getValue());

    assertEquals(JobDetailsValues.HDFS_BYTES_WRITTEN_1,
      jd.getCounters().getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_WRITTEN)
          .getValue());

    assertEquals(JobDetailsValues.SLOTS_MILLIS_MAPS_1,
      jd.getCounters().getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_MAPS)
          .getValue());

    assertEquals(JobDetailsValues.SLOTS_MILLIS_REDUCES_1,
      jd.getCounters().getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_REDUCES)
          .getValue());
  }

  private void confirmHadoopVersion(JobDetails jd, HadoopVersion hv) {
    assertEquals(hv, jd.getHadoopVersion());
  }

  /**
   * Test hadoop1 counters in the {@link JobDetails} populate function
   */
  @Test
  public void testPopulateForHadoop2() {
    JobDetails jd = new JobDetails(null);
    NavigableMap<byte[], byte[]> infoValues = getInfoValues(HadoopVersion.TWO);

    Result result = Mockito.mock(Result.class);
    when(result.getFamilyMap(Constants.INFO_FAM_BYTES)).thenReturn(infoValues);

    jd.populate(result);

    // confirm hadoop2 first
    confirmHadoopVersion(jd, HadoopVersion.TWO);
    // confirm job details
    confirmSomeJobDeets(jd);
    // confirm hadoop2 counters
    confirmHadoop2Counters(jd);
  }


  /**
   * test the get counter value function
   */
  @Test
  public void testGetCounterValueAsLong() {
    CounterMap cm = new CounterMap();
    String cg = Constants.FILESYSTEM_COUNTERS;
    String cname = Constants.FILES_BYTES_READ;
    Long expValue = 1234L;
    Counter c1 = new Counter(cg, cname, expValue);
    cm.add(c1);
    JobDetails jd = new JobDetails(null);
    assertEquals(expValue, jd.getCounterValueAsLong(cm, cg, cname));

    // test non existent counter value
    Long zeroValue = 0L;
    assertEquals(zeroValue,
      jd.getCounterValueAsLong(cm, Constants.JOB_COUNTER_HADOOP2, Constants.SLOTS_MILLIS_MAPS));
  }

  /**
   * Test hadoop1 counters in the {@link JobDetails} populate function
   */
  @Test
  public void testPopulateForHadoop1() {
    JobDetails jd = new JobDetails(null);
    NavigableMap<byte[], byte[]> infoValues = getInfoValues(HadoopVersion.ONE);

    Result result1 = mock(Result.class);
    when(result1.getFamilyMap(Constants.INFO_FAM_BYTES)).thenReturn(infoValues);

    jd.populate(result1);

    // confirm hadoop 1
    confirmHadoopVersion(jd, HadoopVersion.ONE);
    // confirm hadoop1 counters
    confirmHadoop1Counters(jd);
    // confirm job details
    confirmSomeJobDeets(jd);
  }

}