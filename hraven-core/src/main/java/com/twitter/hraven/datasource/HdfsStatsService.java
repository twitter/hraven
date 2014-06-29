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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.QualifiedPathKey;
import com.twitter.hraven.util.FuzzyRowFilter;
import com.twitter.hraven.util.StringUtil;

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
      // path expected to be cleansed at collection/storage time as well
      rowPrefixStr += HdfsConstants.SEP + StringUtil.cleanseToken(pathPrefix);
    }
    LOG.info(" Getting all dirs for cluster " + cluster + " with pathPrefix: " + pathPrefix
        + " for runId " + runId + " encodedRunId: " + encodedRunId + " limit: " + limit
        + " row prefix : " + rowPrefixStr);
    byte[] rowPrefix = Bytes.toBytes(rowPrefixStr);
    Scan scan = createScanWithAllColumns();
    scan.setStartRow(rowPrefix);

    // require that all rows match the prefix we're looking for
    Filter prefixFilter = new WhileMatchFilter(new PrefixFilter(rowPrefix));
    scan.setFilter(prefixFilter);
    // using a large scanner caching value with a small limit can mean we scan a lot more data than
    // necessary, so lower the caching for low limits
    scan.setCaching(Math.min(limit, defaultScannerCaching));
    // we need only the latest cell version
    scan.setMaxVersions(1);

    return createFromScanResults(cluster, null, scan, limit, Boolean.FALSE, 0l, 0l);

  }

  private Scan createScanWithAllColumns() {
    Scan scan = new Scan();
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

    return scan;
  }

  /**
   * Scans the hbase table and populates the hdfs stats
   * @param cluster
   * @param scan
   * @param maxCount
   * @return
   * @throws IOException
   */
  private List<HdfsStats> createFromScanResults(String cluster, String path, Scan scan, int maxCount, boolean checkPath,
    long starttime, long endtime)
      throws IOException {
    Map<HdfsStatsKey, HdfsStats> hdfsStats = new HashMap<HdfsStatsKey, HdfsStats>();
    ResultScanner scanner = null;
    Stopwatch timer = new Stopwatch().start();
    int rowCount = 0;
    long colCount = 0;
    long resultSize = 0;

    try {
      scanner = hdfsUsageTable.getScanner(scan);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          colCount += result.size();
          resultSize += result.getWritableSize();
          rowCount = populateHdfsStats(result, hdfsStats, checkPath, path, starttime, endtime, rowCount);
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

    List<HdfsStats> values = new ArrayList<HdfsStats>(hdfsStats.values());
    // sort so that timestamps are arranged in descending order
    Collections.sort(values);
    return values;
  }

  /**
   * Populates the hdfs stats for a cluster based on the hbase Result
   *
   * For federated hadoop2 clusters, there are multiple namespaces
   * Since the namespace is part of the rowkey, we need to create an
   * hdfs key without namespace so that we can aggregate across namespaces
   *
   * @param hbase scan result
   * @param map of hdfsStats
   */
  private int populateHdfsStats(Result result, Map<HdfsStatsKey, HdfsStats> hdfsStats,
      boolean checkPath, String path, long starttime, long endtime, int rowCount) {
    HdfsStatsKey currentFullKey = hdfsStatsKeyConv.fromBytes(result.getRow());
    QualifiedPathKey qpk = currentFullKey.getQualifiedPathKey();

    // we check for exact match of path
    // since the scan does a prefix match, we need to filter out
    // other paths
    if(checkPath) {
      if(!qpk.getPath().equalsIgnoreCase(StringUtil.cleanseToken(path))) {
        return rowCount;
      }
      // sanity check
      if((currentFullKey.getRunId() < endtime) || (currentFullKey.getRunId() > starttime)) {
        return rowCount;
      }
    }
    // create a hdfs stats key object per path without namespace
    // that will enable aggregating stats across all namespaces
    HdfsStatsKey currentKey = new HdfsStatsKey(qpk.getCluster(), qpk.getPath(),
          currentFullKey.getEncodedRunId());
    HdfsStats currentHdfsStats = hdfsStats.get(currentKey);
    if (currentHdfsStats != null) {
      currentHdfsStats.populate(result);
    } else {
      currentHdfsStats = new HdfsStats(new HdfsStatsKey(currentKey));
      currentHdfsStats.populate(result);
      hdfsStats.put(currentKey, currentHdfsStats);
    }
    return rowCount+1;
  }

  /**
   * this function returns an older timestamp
   * ideally we would be using the daily aggregation table
   * and be simply looking for last run of the aggregation,
   * but right now, in the hourly table, we try to look for
   * certain timestamps in the past instead of scanning the
   * entire table for the last run.
   * @param i (i should always be less than ageMult.size)
   * @param runId
   * @return older runId
   *
   * @throws ProcessingException
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
    if (i >= HdfsConstants.ageMult.length) {
      throw new ProcessingException("Can't look back in time that far " + i + ", only upto "
          + HdfsConstants.ageMult.length);
    }
     long newTs = runId - (HdfsConstants.ageMult[i] *
         HdfsConstants.NUM_SECONDS_IN_A_DAY  + randomizedHourInSeconds);
    LOG.info(" Getting older runId for " + runId + ", returning " + newTs + " since ageMult[" + i + "]="
        + HdfsConstants.ageMult[i] + " randomizedHourInSeconds=" + randomizedHourInSeconds);
    return newTs;

  }

  public List<HdfsStats> getHdfsTimeSeriesStats(String cluster, String path,
      int limit, long starttime, long endtime) throws IOException {

    /* a better way to get timeseries info would be to have an aggregated stats
     * table which can be queried better for 2.0 clusters
     */
    Scan scan = GenerateScanFuzzy(starttime, endtime, cluster, path);
    return createFromScanResults(cluster, path, scan, limit, Boolean.TRUE, starttime, endtime);
  }

  Scan GenerateScanFuzzy(long starttime, long endtime, String cluster, String path)
      throws IOException {

    Scan scan = createScanWithAllColumns();

    String rowKeySuffix =
        HdfsConstants.SEP + cluster + HdfsConstants.SEP + StringUtil.cleanseToken(path);
    String rowKey = HdfsConstants.INVERTED_TIMESTAMP_FUZZY_INFO + rowKeySuffix;
    int fuzzyLength = HdfsConstants.NUM_CHARS_INVERTED_TIMESTAMP + rowKeySuffix.length();

    byte[] fuzzyInfo = new byte[fuzzyLength];

    for (int i = 0; i < HdfsConstants.NUM_CHARS_INVERTED_TIMESTAMP; i++) {
      fuzzyInfo[i] = 1;
    }
    for (int i = HdfsConstants.NUM_CHARS_INVERTED_TIMESTAMP; i < fuzzyLength; i++) {
      fuzzyInfo[i] = 0;
    }

    @SuppressWarnings("unchecked")
    FuzzyRowFilter rowFilter =
        new FuzzyRowFilter(Arrays.asList(new Pair<byte[], byte[]>(Bytes.toBytesBinary(rowKey),
            fuzzyInfo)));

   scan.setFilter(rowFilter);
    String minStartKey = Long.toString(getEncodedRunId(starttime));
    String maxEndKey = Long.toString(getEncodedRunId(endtime));
    LOG.info(starttime + " " + getEncodedRunId(starttime) + " min " + minStartKey + " " + endtime
        + " " + maxEndKey + " " + getEncodedRunId(endtime));
    scan.setStartRow(Bytes.toBytes(minStartKey + rowKeySuffix));
    scan.setStopRow(Bytes.toBytes(maxEndKey + rowKeySuffix));

    LOG.info(" scan: " + scan.toJSON());
    return scan;
  }
}
