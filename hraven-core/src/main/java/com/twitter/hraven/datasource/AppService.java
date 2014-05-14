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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Stopwatch;
import com.twitter.hraven.App;
import com.twitter.hraven.AppKey;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.util.ByteUtil;


/**
 * Reads and writes information about applications
 *
 */
public class AppService {

  private static final Log LOG = LogFactory.getLog(AppService.class);
  private final Configuration conf;
  private final HTable versionsTable;

  public AppService(Configuration hbaseConf) throws IOException {
    this.conf = hbaseConf;
    this.versionsTable = new HTable(conf, Constants.HISTORY_APP_VERSION_TABLE);
  }

  /**
   * scans the app version table to look for jobs that showed up in the given time range
   * creates the flow key that maps to these apps
   *
   * @param cluster
   * @param user
   * @param startTime
   * @param endTime
   * @param limit
   * @return list of flow keys
   * @throws IOException 
   * @throws ProcessingException
   */
  public List<App> getNewApps(JobHistoryService jhs, String cluster,
      String user, long startTime, long endTime, int limit) throws IOException {
    byte[] startRow = null;
    if (StringUtils.isNotBlank(user)) {
      startRow = ByteUtil.join(Constants.SEP_BYTES,
      Bytes.toBytes(cluster), Bytes.toBytes(user));
    } else {
      startRow = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(cluster));
    }
    LOG.info("Reading app version rows start at " + Bytes.toStringBinary(startRow));
    Scan scan = new Scan();
    // start scanning app version table at cluster!user!
    scan.setStartRow(startRow);
    // require that all results match this flow prefix
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filters.addFilter(new WhileMatchFilter(new PrefixFilter(startRow)));

    scan.setFilter(filters);

    List<AppKey> newAppsKeys = new ArrayList<AppKey>();
      try {
        newAppsKeys = createNewAppKeysFromResults(scan, startTime, endTime, limit);
      } catch (IOException e) {
        LOG.error("Caught exception while trying to scan, returning empty list of flows: "
            + e.toString());
      }

      List<App> newApps = new ArrayList<App>();
      for(AppKey ak: newAppsKeys) {
        App anApp = new App(ak);
        List<Flow> flows = jhs.getFlowSeries(ak.getCluster(), ak.getUserName(),
          ak.getAppId(), null, Boolean.FALSE, startTime, endTime, Integer.MAX_VALUE);
        anApp.populateDetails(flows);
        newApps.add(anApp);
      }
    return newApps;

  }

  /**
   * creates a list of appkeys from the hbase scan
   * @param scan
   * @param startTime
   * @param endTime
   * @param maxCount
   * @return list of flow keys
   * @throws IOException
   */
  public List<AppKey> createNewAppKeysFromResults(Scan scan, Long startTime, Long endTime, int maxCount)
          throws IOException {
    ResultScanner scanner = null;
    List<AppKey> newAppsKeys = new ArrayList<AppKey>();
    try {
      Stopwatch timer = new Stopwatch().start();
      int rowCount = 0;
      long colCount = 0;
      long resultSize = 0;
      scanner = versionsTable.getScanner(scan);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          resultSize += result.getWritableSize();
          AppKey appKey = getNewAppKeysFromResult(result, startTime, endTime);
          if(appKey != null) {
            newAppsKeys.add(appKey);
          }
          if (newAppsKeys.size() >= maxCount) {
            break;
          }
        }
      }
      timer.stop();
      LOG.info(" Fetched from hbase " + rowCount + " rows, " + colCount + " columns, "
          + resultSize + " bytes ( " + resultSize / (1024 * 1024)
          + ") MB, in total time of " + timer);
      } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    return newAppsKeys;
  }

  /**
   * constructs a flow key from the result set based on cluster, user, appId
   * picks those results that satisfy the time range criteria
   * @param result
   * @param startTime
   * @param endTime
   * @return flow key
   * @throws IOException
   */
  private AppKey getNewAppKeysFromResult(Result result, Long startTime, Long endTime)
      throws IOException {

    byte[] rowKey = result.getRow();
    byte[][] keyComponents = ByteUtil.split(rowKey, Constants.SEP_BYTES);
    String cluster = Bytes.toString(keyComponents[0]);
    String user = Bytes.toString(keyComponents[1]);
    String appId = Bytes.toString(keyComponents[2]);

    NavigableMap<byte[],byte[]> valueMap = result.getFamilyMap(Constants.INFO_FAM_BYTES);
    Long runId = Long.MAX_VALUE;
    for (Map.Entry<byte[],byte[]> entry : valueMap.entrySet()) {
      Long tsl = Bytes.toLong(entry.getValue()) ;
      // get the earliest runid, which indicates the first time this app ran
      if (tsl < runId) {
        runId = tsl;
      }
    }
    if((runId >= startTime) && (runId <= endTime)) {
        AppKey ak = new AppKey(cluster, user, appId);
        return ak;
    }
    return null;
  }

}
