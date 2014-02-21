/*
 * Copyright 2014 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.util.StringUtil;

/**
 * Round-trip testing for storage and retrieval of data in job_history table.
 */
public class TestHdfsStatsService {
  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(TestHdfsStatsService.class);
  private static HBaseTestingUtility UTIL;
  private static HTable ht;
  final int testDataSize = 6;
  private static ArrayList<String> ownerList = new ArrayList<String>();
  private static List<String> pathList = new ArrayList<String>();
  private static List<Long> fc = new ArrayList<Long>();
  private static ArrayList<Long> sc = new ArrayList<Long>();
  private static ArrayList<Long> ac = new ArrayList<Long>();
  private static ArrayList<Long> dc = new ArrayList<Long>();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);
    ht = new HTable(UTIL.getConfiguration(), HdfsConstants.HDFS_USAGE_TABLE);

    ownerList.add(0, "user1");
    ownerList.add(1, "user2");
    ownerList.add(2, "user3");
    ownerList.add(3, "user1");
    ownerList.add(4, "user2");
    ownerList.add(5, "user6");

    pathList.add(0, "dir2");
    pathList.add(1, "dir3");
    pathList.add(2, "dir4");
    pathList.add(3, "abc");
    pathList.add(4, "abcde2");
    pathList.add(5, "dir5");

    fc.add(0, 10L);
    fc.add(1, 120L);
    fc.add(2, 12345L);
    fc.add(3, 8000L);
    fc.add(4, 98L);
    fc.add(5, 99999L);

    sc.add(0, 1024567L);
    sc.add(1, 9000120L);
    sc.add(2, 992112345L);
    sc.add(3, 128000L);
    sc.add(4, 98L);
    sc.add(5, 811111199999L);

    ac.add(0, 210L);
    ac.add(1, 2120L);
    ac.add(2, 1235L);
    ac.add(3, 18000L);
    ac.add(4, 22298L);
    ac.add(5, 9L);

    dc.add(0, 3L);
    dc.add(1, 10L);
    dc.add(2, 45L);
    dc.add(3, 100L);
    dc.add(4, 18L);
    dc.add(5, 1109L);
  }

  @Test
  public void TestGetLastHourInvertedTimestamp() throws IOException {
    long ts = 1391896800L;
    long expectedTop = Long.MAX_VALUE - ts;
    long actualTop = HdfsStatsService.getEncodedRunId(ts);
    assertEquals(actualTop, expectedTop);
  }

  @Test
  public void TestGetOlderRunId() {
    long ts = 1392123600L;
    int retryCount = 0;
    for (int i = 0; i < HdfsConstants.ageMult.length; i++) {
      long oldts = HdfsStatsService.getOlderRunId(retryCount, ts);
      long diff = (ts - oldts) / HdfsConstants.NUM_SECONDS_IN_A_DAY;
      assertTrue(diff <= HdfsConstants.ageMult[retryCount]);
      retryCount++;
    }
  }


  @Test
  public void TestGetAllDirsPathSpecialChars() throws IOException {
    HdfsStatsService hs = new HdfsStatsService(UTIL.getConfiguration());
    long ts = 1392217200L;
    String cluster1 = "cluster1";
    String rootPath = "/dirSpecialChars/";
    String pathPrefix = "dir2 dir2!!! dir3";
    loadHdfsUsageStats(cluster1, rootPath + pathPrefix, (Long.MAX_VALUE - ts), 10L, 10L,
      "user1", 1234L, 20L, ht);
    List<HdfsStats> h1 = hs.getAllDirs(cluster1, rootPath + pathPrefix, 10, ts);
    assertEquals(h1.size(), 1);
    HdfsStats h2 = h1.get(0);
    String path = h2.getHdfsStatsKey().getQualifiedPathKey().getPath();
    String[] dirs = path.split("/");
    assertEquals(dirs.length, 3);
    assertEquals(dirs[2], StringUtil.cleanseToken(pathPrefix));
    assertEquals(h2.getAccessCountTotal(), 20L);
    assertEquals(h2.getHdfsStatsKey().getRunId(),  ts - (ts % 3600));
    assertEquals(h2.getDirCount(), 10L);
    assertEquals(h2.getFileCount(), 10L);
    assertEquals(h2.getOwner(), "user1");
    assertEquals(h2.getSpaceConsumed(), 1234L);

  }

  @Test
  public void TestGetAllDirsEmpty() throws IOException {
    HdfsStatsService hs = new HdfsStatsService(UTIL.getConfiguration());
    long ts = 1392217200L;
    String cluster1 = "cluster1";
    String pathPrefix = "/dir1/";
    List<HdfsStats> h1 = hs.getAllDirs(cluster1, pathPrefix, 10, (Long.MAX_VALUE - ts));
    assertEquals(h1.size(), 0);
  }

  private void loadHdfsData(long runId, String cluster1, String pathPrefix) throws IOException {
    for (int i = 0; i < testDataSize; i++) {
      loadHdfsUsageStats(cluster1, pathPrefix + pathList.get(i), runId, fc.get(i), dc.get(i),
        ownerList.get(i), sc.get(i), ac.get(i), ht);
    }
  }

  @Test(expected=ProcessingException.class)
  public void testGetOlderRunIdsException() {
    int i = HdfsConstants.ageMult.length + 10;
    HdfsStatsService.getOlderRunId(i, System.currentTimeMillis()/1000);
  }

  private void assertHdfsStats(HdfsStats h2, String cluster1, long runId) {
    String path = h2.getHdfsStatsKey().getQualifiedPathKey().getPath();
    String[] dirs = path.split("/");
    assertEquals(dirs.length, 3);
    int index = pathList.indexOf(dirs[2]);
    assertTrue(index >= 0);
    assertEquals((Long) h2.getAccessCountTotal(), ac.get(index));
    assertEquals(h2.getHdfsStatsKey().getRunId(), runId);
    assertEquals((Long) h2.getDirCount(), dc.get(index));
    assertEquals((Long) h2.getFileCount(), fc.get(index));
    assertEquals(h2.getOwner(), ownerList.get(index));
    assertEquals((Long) h2.getSpaceConsumed(), sc.get(index));
  }

  @Test
  public void TestGetAllDirs() throws IOException {
    HdfsStatsService hs = new HdfsStatsService(UTIL.getConfiguration());
    long ts = 1392217200L;
    String cluster1 = "cluster1";
    String pathPrefix = "/dir1/";
    long encodedRunId = (Long.MAX_VALUE - ts);
    loadHdfsData(encodedRunId, cluster1, pathPrefix);
    List<HdfsStats> h1 = hs.getAllDirs(cluster1, pathPrefix, 10, ts);
    assertEquals(h1.size(), testDataSize);
    for (int i = 0; i < h1.size(); i++) {
      HdfsStats h2 = h1.get(i);
      assertHdfsStats(h2, cluster1, ts);
    }

    /**
     * limit the size of response returned 
     */
    List<HdfsStats> h2 = hs.getAllDirs(cluster1, pathPrefix, 2, ts);
    assertEquals(h2.size(), 2);

    /**
     * check for a non existent path
     */
    List<HdfsStats> h3 = hs.getAllDirs(cluster1, "nonexistentpath", 100, ts);
    assertEquals(h3.size(), 0);

  }

  /**
   * loads the stats in the mini cluster
   * @param cluster1
   * @param path
   * @param fc fileCounts
   * @param dc directoryCounts
   * @param owner
   * @param sc spaceConsumed
   * @param ac accessCounts
   * @param htable
   * @throws IOException
   */
  private void loadHdfsUsageStats(String cluster1, String path, long encodedRunId, long fc, long dc,
      String owner, long sc, long ac, HTable ht) throws IOException {
    HdfsStatsKey key = new HdfsStatsKey(cluster1, StringUtil.cleanseToken(path), encodedRunId);
    HdfsStatsKeyConverter hkc = new HdfsStatsKeyConverter();
    Put p = new Put(hkc.toBytes(key));
    p.add(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.FILE_COUNT_COLUMN_BYTES,
      Bytes.toBytes(fc));
    p.add(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.DIR_COUNT_COLUMN_BYTES,
      Bytes.toBytes(dc));
    p.add(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.ACCESS_COUNT_TOTAL_COLUMN_BYTES,
      Bytes.toBytes(ac));
    p.add(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.OWNER_COLUMN_BYTES, Bytes.toBytes(owner));
    p.add(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.SPACE_CONSUMED_COLUMN_BYTES,
      Bytes.toBytes(sc));
    ht.put(p);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
