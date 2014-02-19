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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HdfsStatsKey;


/**
 * Service that accesses the hdfs stats tables and populates the HdfsStats object
 */
public class HdfsStatsService {
  private static Log LOG = LogFactory.getLog(HdfsStatsService.class);

  private final Configuration myConf;
  private final HTable hdfsUsageTable;

  private final int defaultScannerCaching;
  private final HdfsStatsKeyConverter hdfsStatsKeyConv;

  public HdfsStatsService(Configuration conf) throws IOException {
    this.myConf = conf;
    this.hdfsUsageTable = new HTable(myConf, HdfsConstants.HDFS_USAGE_TABLE_BYTES);
    this.defaultScannerCaching = myConf.getInt("hbase.client.scanner.caching", 100);
    LOG.info(" in HdfsStatsService constuctor " + Bytes.toString(hdfsUsageTable.getTableName()));
    hdfsStatsKeyConv = new HdfsStatsKeyConverter();
  }

  /**
   * returns the inverted timestamp for a given timestamp
   * @param now
   * @return
   */
  public static long getEncodedRunId(long now) {
    // get top of the hour
    long lastHour = now - (now % 3600);
    // return inverted timestamp
    return (Long.MAX_VALUE - lastHour);
  }

  /**
   * Gets hdfs stats about all dirs on the given cluster
   * @param cluster
   * @param pathPrefix
   * @param limit
   * @param runId
   * @return list of hdfs stats
   * @throws IOException
   */
  public List<HdfsStats> getAllDirs(String cluster, String pathPrefix, int limit, long runId)
      throws IOException {
    long encodedRunId = getEncodedRunId(runId);
    String rowPrefixStr = Long.toString(encodedRunId) + HdfsConstants.SEP + cluster;
    if (StringUtils.isNotEmpty(pathPrefix)) {
      rowPrefixStr += HdfsConstants.SEP + pathPrefix;
    }
    LOG.info(" Getting all dirs for cluster " + cluster + " with pathPrefix: " + pathPrefix
        + " for runId " + runId + " encodedRunId: " + encodedRunId + " limit: " + limit
        + " row prefix : " + rowPrefixStr);
    byte[] rowPrefix = Bytes.toBytes(rowPrefixStr);
    Scan scan = new Scan();
    scan.setStartRow(rowPrefix);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.FILE_COUNT_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.DIR_COUNT_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.ACCESS_COUNT_TOTAL_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.ACCESS_COST_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.STORAGE_COST_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.SPACE_CONSUMED_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.TMP_FILE_COUNT_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.TMP_SPACE_CONSUMED_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.TRASH_FILE_COUNT_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES,
      HdfsConstants.TRASH_SPACE_CONSUMED_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.OWNER_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.QUOTA_COLUMN_BYTES);
    scan.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, HdfsConstants.SPACE_QUOTA_COLUMN_BYTES);

    // using a large scanner caching value with a small limit can mean we scan a lot more data than
    // necessary, so lower the caching for low limits
    scan.setCaching(Math.min(limit, defaultScannerCaching));
    // require that all rows match the prefix we're looking for
    Filter prefixFilter = new WhileMatchFilter(new PrefixFilter(rowPrefix));
    scan.setFilter(prefixFilter);
    return createFromScanResults(cluster, scan, limit);

  }

  /**
   * Scans the hbase table and populates the hdfs stats
   * @param cluster
   * @param scan
   * @param maxCount
   * @return
   * @throws IOException
   */
  private List<HdfsStats> createFromScanResults(String cluster, Scan scan, int maxCount)
      throws IOException {
    List<HdfsStats> hdfsStats = new LinkedList<HdfsStats>();
    ResultScanner scanner = null;
    Stopwatch timer = new Stopwatch().start();
    int rowCount = 0;
    long colCount = 0;
    long resultSize = 0;

    try {
      scanner = hdfsUsageTable.getScanner(scan);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          resultSize += result.getWritableSize();
          populateHdfsStats(result, hdfsStats);
          // return if we've already hit the limit
          if (rowCount >= maxCount) {
            break;
          }
        }
      }
    } finally {
      timer.stop();
      LOG.info("In createFromScanResults For cluster " + cluster + " Fetched from hbase " + rowCount + " rows, " + colCount
          + " columns, " + resultSize + " bytes ( " + resultSize / (1024 * 1024)
          + ") MB, in total time of " + timer);
      if (scanner != null) {
        scanner.close();
      }
    }

    return hdfsStats;
  }

  
  /**
   * Gets from the hbase table and populates the hdfs stats
   * @param cluster
   * @param gets
   * @param maxCount
   * @return
   * @throws IOException
   */
  private List<HdfsStats> createFromGetResults(String cluster, List<Get> gets, int maxCount)
      throws IOException {
    List<HdfsStats> hdfsStats = new LinkedList<HdfsStats>();
    Result[] allResults = null;
    Stopwatch timer = new Stopwatch().start();
    long resultSize = 0;
    int rowCount = 0;
    long colCount = 0;

    try {
      allResults = hdfsUsageTable.get(gets);
      for (Result result : allResults) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          resultSize += result.getWritableSize();

          populateHdfsStats(result, hdfsStats);
          // return if we've already hit the limit
          if (rowCount >= maxCount) {
            break;
          }
        }
      }
    } finally {
      timer.stop();
      LOG.info("In createFromGetResults: For cluster " + cluster + " Fetched from hbase " + rowCount + " rows, " + colCount
          + " columns, " + resultSize + " bytes ( " + resultSize / (1024 * 1024)
          + ") MB, in total time of " + timer);
    }

    return hdfsStats;
  }

  private void populateHdfsStats(Result result, List<HdfsStats> hdfsStats) {
    HdfsStatsKey currentKey = hdfsStatsKeyConv.fromBytes(result.getRow());
    HdfsStats currentHdfsStats = new HdfsStats(new HdfsStatsKey(currentKey));
    currentHdfsStats.populate(result);
    hdfsStats.add(currentHdfsStats);
  }

  /**
   * this function returns an older timestamp
   * ideally we would be using the daily aggregation table
   * and be simply looking for last run of the aggregation,
   * but right now, in the hourly table, we try to look for
   * certain timestamps in the past instead of scanning the
   * entire table for the last run.
   * @param i
   * @param runId
   * @return older runId
   */
  public static long getOlderRunId(int i, long runId) {

    /**
     * try to pick a randomized hour around that old date
     * instead of picking exactly the same hour
     */
    int randomizedHourInSeconds = (int) (Math.random() * 23) * 3600;
    /**
     * consider the day before old date and add randomizedHour
     * for example, for retryCount = 0
     * pick a day which is 2 days before ageMult[0], that is
     * 2 days before the current date in consideration
     * and add the randomized hour say 13 hours, so that gives an
     * hour belonging to one day before current date in consideration
     */
     long newTs = runId - (HdfsConstants.ageMult[i] *
         HdfsConstants.NUM_SECONDS_IN_A_DAY  + randomizedHourInSeconds);
    LOG.info(" Getting older runId for " + runId + ", returning " + newTs + " since ageMult[" + i + "]="
        + HdfsConstants.ageMult[i] + " randomizedHourInSeconds=" + randomizedHourInSeconds);
    return newTs;

  }

  public List<HdfsStats> getHdfsTimeSeriesStats(String cluster, String path, String attribute,
      int limit, long starttime, long endtime) throws IOException {

    List<Get> gets = GenerateGets(starttime, endtime, cluster, path, attribute, limit);
    return createFromGetResults(cluster, gets, limit);
  }

  private List<Get> GenerateGets(long starttime, long endtime, String cluster,
        String path, String attribute, int limit) {
    long encodedRunIdStart = 0l;
    int count = 0;
    byte[] attributeBytes = Bytes.toBytes(attribute);
    List<Get> gets = new ArrayList<Get>();
    LOG.info(" Starting Getting all timeseries stats for cluster " + cluster + " with path: " + path
      + " for starttime " + starttime + " encodedRunId: " + encodedRunIdStart + " limit: " + limit
      + " endtime " + endtime);
    while((starttime >= endtime) && (count <= limit)) {
      encodedRunIdStart = getEncodedRunId(starttime);
      StringBuilder rowkey = new StringBuilder();
      rowkey.append(Long.toString(encodedRunIdStart));
      rowkey.append(HdfsConstants.SEP);
      rowkey.append(cluster);
      rowkey.append(HdfsConstants.SEP);
      rowkey.append(path);
      String startRowPrefixStr = rowkey.toString();
      byte[] rowPrefix = Bytes.toBytes(startRowPrefixStr);
      Get g = new Get(rowPrefix);
      g.addColumn(HdfsConstants.DISK_INFO_FAM_BYTES, attributeBytes);
      gets.add(g);
      count++;
      // go to previous hour
      starttime -= 3600;
    }
    return gets;
  }
}
