/*
Copyright 2014 Twitter, Inc.

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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.AggregationConstants;
import com.twitter.hraven.AppSummary;
import com.twitter.hraven.Constants;
import com.twitter.hraven.GenerateFlowTestData;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.HRavenTestUtil;
import com.twitter.hraven.util.ByteUtil;

public class TestAppSummaryService {

  private static Log LOG = LogFactory.getLog(TestAppSummaryService.class);
  private static HBaseTestingUtility UTIL;
  private static JobHistoryByIdService idService;
  private static GenerateFlowTestData flowDataGen;
  private static Connection conn;
  private static Table historyTable;
  private static Table dailyAggTable;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);

    conn = ConnectionFactory.createConnection(UTIL.getConfiguration());

    historyTable = conn.getTable(TableName.valueOf(Constants.HISTORY_TABLE_BYTES));
    idService = new JobHistoryByIdService(UTIL.getConfiguration());
    flowDataGen = new GenerateFlowTestData();
    dailyAggTable = conn.getTable(TableName.valueOf(AggregationConstants.AGG_DAILY_TABLE_BYTES));
  }

  @Test
  public void testNewJobs() throws Exception {

    Configuration c = UTIL.getConfiguration();

    String cluster1 = "newJobsClusterName";
    String appId = "getNewJobs";
    String appId2 = "getNewJobs2";
    String user = "testuser";

    JobHistoryService jhs = null;
    AppVersionService service = null;
    AppSummaryService as = null;
    try {
      jhs = new JobHistoryService(UTIL.getConfiguration());
      service = new AppVersionService(c);
      as = new AppSummaryService(c);

      // check adding versions in order
      service.addVersion(cluster1, user, appId, "v1", 10L);
      flowDataGen.loadFlow(cluster1, user, appId, 10L, "v1", 3, 10, idService, historyTable);
      service.addVersion(cluster1, user, appId, "v2", 20L);
      flowDataGen.loadFlow(cluster1, user, appId, 20L, "v2", 3, 10, idService, historyTable);
      // Since flow has its own unit tests
      // we don't test the get flow series as part of testing this new jobs call
      List<AppSummary> fl = as.getNewApps(jhs, cluster1, user, 0L, 15L, 100);
      assertNotNull(fl);
      assertEquals(1, fl.size());
      service.addVersion(cluster1, user, appId, "v3", 30L);
      flowDataGen.loadFlow(cluster1, user, appId, 30L, "v3", 3, 10, idService, historyTable);
      service.addVersion(cluster1, user, appId, "v2.5", 25L);
      flowDataGen.loadFlow(cluster1, user, appId, 25L, "v2.5", 3, 10, idService, historyTable);
      fl = as.getNewApps(jhs, cluster1, user, 20L, 35L, 100);
      assertNotNull(fl);
      // check that nothing is returned since this app first showed up at timestamp 10
      assertEquals(0, fl.size());
      service.addVersion(cluster1, user, appId2, "v102", 23L);
      flowDataGen.loadFlow(cluster1, user, appId, 23L, "v102", 3, 10, idService, historyTable);
      fl = as.getNewApps(jhs, cluster1, user, 20L, 35L, 5);
      assertNotNull(fl);
      // check that appId2 is returned
      assertEquals(1, fl.size());
      assertEquals(appId2, fl.get(0).getKey().getAppId());
      // now check for entire time range
      fl = as.getNewApps(jhs, cluster1, user, 0L, 35L, 5);
      assertNotNull(fl);
      // check that both apps are returned
      assertEquals(2, fl.size());
      for (int i = 0; i < fl.size(); i++) {
        String anAppId = fl.get(i).getKey().getAppId();
        if(!(appId.equals(anAppId)) && !(appId2.equals(anAppId))) {
          throw new AssertionError("Could not find the right apps as expected");
        }
      }
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
      try {
        if (service != null) {
          service.close();
        }
      } catch (IOException ignore) {
      }
      try {
        if (jhs != null) {
          jhs.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testGetDayTimestamp() throws IOException {
    long ts = 1402698420000L;
    long expectedTop = 1402617600000L;
    AppSummaryService as = null;
    try {
      as = new AppSummaryService(UTIL.getConfiguration());
      assertEquals((Long) expectedTop, (Long) as.getTimestamp(ts,
          AggregationConstants.AGGREGATION_TYPE.DAILY));
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testGetWeekTimestamp() throws IOException {
    long ts = 1402698420000L;
    long expectedTop = 1402185600000L;
    AppSummaryService as = null;
    try {
      as = new AppSummaryService(UTIL.getConfiguration());
      assertEquals((Long) expectedTop, (Long) as.getTimestamp(ts,
          AggregationConstants.AGGREGATION_TYPE.WEEKLY));
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test(expected=ProcessingException.class)
  public void testGetNumberRuns() throws IOException {
    Map<byte[], byte[]> r = new HashMap<byte[], byte[]>();
    AppSummaryService as = null;
    try {
      as = new AppSummaryService(UTIL.getConfiguration());
      assertEquals(0, as.getNumberRunsScratch(r));
      byte[] key = Bytes.toBytes("abc");
      byte[] value = Bytes.toBytes(10L);
      r.put(key, value);
      key = Bytes.toBytes("xyz");
      value = Bytes.toBytes(102L);
      r.put(key, value);
      assertEquals(2, as.getNumberRunsScratch(r));
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testCreateQueueListValue() throws IOException {
    JobDetails jd = new JobDetails(null);
    jd.setQueue("queue1");
    byte[] qb = Bytes.toBytes("queue2!queue3!");
    Cell existingQueuesCell =
        CellUtil.createCell(Bytes.toBytes("rowkey"), Constants.INFO_FAM_BYTES,
            Constants.HRAVEN_QUEUE_BYTES, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(), qb);
    AppSummaryService as = new AppSummaryService(null);
    try {
      String qlist = as.createQueueListValue(jd, Bytes.toString(CellUtil.cloneValue(existingQueuesCell)));
      assertNotNull(qlist);
      String expQlist = "queue2!queue3!queue1!";
      assertEquals(expQlist, qlist);

      jd.setQueue("queue3");
      qlist = as.createQueueListValue(jd, Bytes.toString(CellUtil.cloneValue(existingQueuesCell)));
      assertNotNull(qlist);
      expQlist = "queue2!queue3!";
      assertEquals(expQlist, qlist);
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  private JobDetails createJobDetails(int mult, long runId) {
    JobDetails jd =
        new JobDetails(new JobKey("cluster", "user", "appid", runId, "job_1402359360000_999"
            + Integer.toString(mult)));
    jd.setTotalMaps(10L * mult);
    jd.setTotalReduces(10L * mult);
    jd.setMapSlotMillis(20L * mult);
    jd.setReduceSlotMillis(222L * mult);
    jd.setMegabyteMillis(33L * mult);
    jd.setCost(200.0 * mult);
    jd.setQueue("queue_" + Integer.toString(mult));
    return jd;
  }

  /**
   * inserts job details into aggregation table then scans it and looks for expected columns and
   * values
   * @throws IOException
   */
  @Test
  public void testAggregateJobDetailsDailyAndGetAllApps() throws IOException {
    JobDetails jd = createJobDetails(1, 1402704960000L);
    AppSummaryService as = null;
    try {
      as = new AppSummaryService(UTIL.getConfiguration());
      as.aggregateJobDetails(jd, AggregationConstants.AGGREGATION_TYPE.DAILY);
      jd = createJobDetails(2, 1402712160000L);
      as.aggregateJobDetails(jd, AggregationConstants.AGGREGATION_TYPE.DAILY);
      Scan scan = new Scan();
      long startTime = 1402704000000L;
      long endTime = 1402704000000L;
      byte[] startRow =
          ByteUtil.join(Constants.SEP_BYTES, Bytes.toBytes("cluster"),
              Bytes.toBytes(Long.MAX_VALUE - endTime - 1));
      LOG.trace(endTime + " startrow: Long.MAX_VALUE - endTime) " + (Long.MAX_VALUE - endTime)
          + new Date(endTime));
      byte[] endRow =
          ByteUtil.join(Constants.SEP_BYTES, Bytes.toBytes("cluster"),
              Bytes.toBytes(Long.MAX_VALUE - startTime + 1));
      LOG.trace(startTime + " endrow: Long.MAX_VALUE - startTime) " + (Long.MAX_VALUE - startTime)
          + new Date(startTime));
      scan.setStartRow(startRow);
      scan.setStopRow(endRow);
      int rowCount = 0;
      int colCount = 0;
      ResultScanner scanner = dailyAggTable.getScanner(scan);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          byte[] rowKey = result.getRow();
          byte[][] keyComponents = ByteUtil.split(rowKey, Constants.SEP_BYTES);
          assertEquals(4, keyComponents.length);
          assertEquals("cluster", Bytes.toString(keyComponents[0]));
          assertEquals((Long.MAX_VALUE - 1402704000000L), Bytes.toLong(keyComponents[1]));
          assertEquals("user", Bytes.toString(keyComponents[2]));
          assertEquals("appid", Bytes.toString(keyComponents[3]));
          long slotmillismaps =
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.SLOTS_MILLIS_MAPS_BYTES).getValue());
          assertEquals(60L, slotmillismaps);
          assertEquals(
              666L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.SLOTS_MILLIS_REDUCES_BYTES).getValue()));
          assertEquals(
              new Double(600.0),
              (Double) Bytes.toDouble(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.JOBCOST_BYTES).getValue()));
          assertEquals(
              99L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.MEGABYTEMILLIS_BYTES).getValue()));
          assertEquals(
              2L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.NUMBER_RUNS_BYTES).getValue()));
          assertEquals(
              "queue_1!queue_2!",
              Bytes.toString(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.HRAVEN_QUEUE_BYTES).getValue()));
          assertEquals(
              2L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.TOTAL_JOBS_BYTES).getValue()));
          assertEquals(
              30L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.TOTAL_MAPS_BYTES).getValue()));
          assertEquals(
              30L,
              Bytes.toLong(result.getColumnLatest(
                  AggregationConstants.INFO_FAM_BYTES,
                  AggregationConstants.TOTAL_REDUCES_BYTES).getValue()));

          Map<byte[], byte[]> valueMap = result.getFamilyMap(
              AggregationConstants.SCRATCH_FAM_BYTES);
          assertEquals(2, valueMap.size());
          assertTrue(valueMap.containsKey(Bytes.toBytes(1402704960000L)));
          assertTrue(valueMap.containsKey(Bytes.toBytes(1402712160000L)));
          assertEquals(1, Bytes.toLong(valueMap.get(Bytes.toBytes(1402704960000L))));
          assertEquals(1, Bytes.toLong(valueMap.get(Bytes.toBytes(1402712160000L))));
        }
      }
      assertEquals(1, rowCount);
      assertEquals(13, colCount);

      Set<String> qSet = new HashSet<String>();
      qSet.add("queue_1");
      qSet.add("queue_2");
      List<AppSummary> a = as.getAllApps("cluster", "", 1402704960000L, 1402712160000L, 100);
      assertNotNull(a);
      assertEquals(1, a.size());
      assertEquals("appid", a.get(0).getKey().getAppId());
      assertEquals(60L, a.get(0).getMapSlotMillis());
      assertEquals(666L, a.get(0).getReduceSlotMillis());
      assertEquals(new Double(600.0), (Double) a.get(0).getCost());
      assertEquals(99L, a.get(0).getMbMillis());
      assertEquals(2L, a.get(0).getNumberRuns());
      assertEquals(qSet, a.get(0).getQueue());
      assertEquals(2L, a.get(0).getJobCount());
      assertEquals(30L, a.get(0).getTotalMaps());
      assertEquals(30L, a.get(0).getTotalReduces());
    } finally {
      try {
        if (as != null) {
          as.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      historyTable.close();
    } catch (Exception ignore) {
    }
    try {
      dailyAggTable.close();
    } catch (Exception ignore) {
    }
    try {
      idService.close();
    } catch (Exception ignore) {
    }
    try {
      conn.close();
    } catch (Exception ignore) {
    }
    UTIL.shutdownMiniCluster();
  }
}

