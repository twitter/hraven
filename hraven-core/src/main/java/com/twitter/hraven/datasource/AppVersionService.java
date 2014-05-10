/*
Copyright 2013 Twitter, Inc.

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.twitter.hraven.Constants;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.util.ByteUtil;

/**
 * Reads and writes information about the mapping of application IDs
 * to version numbers.
 */
public class AppVersionService {

  private static Log LOG = LogFactory.getLog(AppVersionService.class);

  @SuppressWarnings("unused")
  private final Configuration conf;
  private final HTable versionsTable;

  public AppVersionService(Configuration conf) throws IOException {
    this.conf = conf;
    this.versionsTable = new HTable(conf, Constants.HISTORY_APP_VERSION_TABLE);
  }

  /**
   * Returns the most recent version ID for the given application.
   *
   * @param cluster
   * @param user
   * @param appId
   * @return the most recent version ID or {@code null} if no versions are found
   * @throws IOException
   */
  public String getLatestVersion(String cluster, String user, String appId)
      throws IOException {
    Get get = new Get(getRowKey(cluster, user, appId));
    List<VersionInfo> versions = Lists.newArrayList();
    Result r = this.versionsTable.get(get);
    if (r != null && !r.isEmpty()) {
      for (KeyValue kv : r.list()) {
        versions.add(
            new VersionInfo(Bytes.toString(kv.getQualifier()), Bytes.toLong(kv.getValue())) );
      }
    }

    if (versions.size() > 0) {
      Collections.sort(versions);
      return versions.get(0).getVersion();
    }

    return null;
  }

  /**
   * Returns the list of distinct versions for the given application
   * sorted in reverse chronological order
   *
   * @param cluster
   * @param user
   * @param appId
   * @return the list of versions sorted in reverse chronological order
   * (the list will be empty if no versions are found)
   * @throws IOException
   */
  public List<VersionInfo> getDistinctVersions(String cluster, String user, String appId)
      throws IOException {
    Get get = new Get(getRowKey(cluster, user, appId));
    List<VersionInfo> versions = Lists.newArrayList();
    Long ts = 0L;
    Result r = this.versionsTable.get(get);
    if (r != null && !r.isEmpty()) {
      for (KeyValue kv : r.list()) {
        ts = 0L;
        try {
          ts = Bytes.toLong(kv.getValue());
          versions.add(
              new VersionInfo(Bytes.toString(kv.getQualifier()), ts) );
        }
        catch (IllegalArgumentException e1 ) {
          // Bytes.toLong may throw IllegalArgumentException, although unlikely.
          LOG.error("Caught conversion error while converting timestamp to long value "
              + e1.getMessage());
            // rethrow the exception in order to propagate it
            throw e1;
        }
      }
    }

    if (versions.size() > 0) {
      Collections.sort(versions);
    }

    return versions;
 }

  /**
   * Adds an entry for the given version, if it does not already exist.  If the
   * given timestamp is earlier than the currently stored timestamp for the version,
   * it will be updated.
   *
   * @param cluster cluster identifier (cluster@identifier)
   * @param user user name
   * @param appId application identifier
   * @param version version identifier
   * @param timestamp timestamp to store with this version (only the earliest timestamp is stored)
   * @return {@code true} if a new version entry was added, {@code false}
   * if the version already existed
   */
  public boolean addVersion(String cluster, String user, String appId,
                           String version, long timestamp) throws IOException {
    boolean updated = false;

    // check if the version already exists
    byte[] rowKey = getRowKey(cluster, user, appId);
    byte[] versionCol = Bytes.toBytes(version);

    int attempts = 0;
    // retry up to this many times for checkAndPut failures
    int maxAttempts = 3;
    boolean checkForUpdate = true;

    while (checkForUpdate && attempts < maxAttempts) {
      attempts++;
      // values for conditional update
      Put p = null;
      byte[] expectedValue = null;

      Get get = new Get(rowKey);
      get.addColumn(Constants.INFO_FAM_BYTES, versionCol);
      Result r = this.versionsTable.get(get);
      if (r != null && !r.isEmpty()) {
        byte[] storedValue = r.getValue(Constants.INFO_FAM_BYTES, versionCol);
        long storedTS = Bytes.toLong(storedValue);
        if (timestamp < storedTS) {
          // update the stored timestamp to our earlier value
          p = new Put(rowKey);
          p.add(Constants.INFO_FAM_BYTES, versionCol, Bytes.toBytes(timestamp));
          expectedValue = storedValue;
        } else {
          // version exists and exceeds our value, no update necessary
          checkForUpdate = false;
        }
      } else {
        // no stored value
        p = new Put(rowKey);
        p.add(Constants.INFO_FAM_BYTES, versionCol, Bytes.toBytes(timestamp));
      }

      if (p != null) {
        // we have an updated value to add
        updated = this.versionsTable.checkAndPut(
            rowKey, Constants.INFO_FAM_BYTES, versionCol, expectedValue, p);
        checkForUpdate = !updated;
        if (!updated) {
          LOG.warn("Update of cluster="+cluster+", user="+user+", app="+appId+
              ", version="+version+" to timestamp "+timestamp+
              " failed because currently set value changed!"+
              " (attempt "+attempts+" of "+maxAttempts+")");
        }
      }
    }

    return updated;
  }

  /**
   * Close the underlying HTable reference to free resources
   * @throws IOException
   */
  public void close() throws IOException {
    if (this.versionsTable != null) {
      this.versionsTable.close();
    }
  }

  private byte[] getRowKey(String cluster, String user, String appId) {
    String keyString = new StringBuilder(cluster)
        .append(Constants.SEP).append(user)
        .append(Constants.SEP).append(appId).toString();
    return Bytes.toBytes(keyString);
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
   * @param queueCapacity
   * @param jhs
   * @return list of flows
   * @throws ProcessingException
   */
  public List<FlowKey> getNewAppsKeys(String cluster, String user, Long startTime,
      Long endTime, int limit)
          throws ProcessingException {
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
    // start scanning history at cluster!user!app!run!
    scan.setStartRow(startRow);
    // require that all results match this flow prefix
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filters.addFilter(new WhileMatchFilter(new PrefixFilter(startRow)));

    scan.setFilter(filters);

    List<FlowKey> newAppsKeys = new ArrayList<FlowKey>();
      try {
        newAppsKeys = createFromResults(scan, startTime, endTime, limit);
      } catch (IOException e) {
        LOG.error("Caught exception while trying to scan, returning empty list of flows: "
            + e.toString());
      }

    return newAppsKeys;
  }

  /**
   * creates a list of appkeys from the hbase scan
   * @param scan
   * @param startTime
   * @param endTime
   * @param maxCount
   * @return
   * @throws IOException
   */
  private List<FlowKey> createFromResults(Scan scan, Long startTime, Long endTime, int maxCount)
          throws IOException {
    ResultScanner scanner = null;
    List<FlowKey> newAppsKeys = new ArrayList<FlowKey>();
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
          // empty runId is special cased -- we need to treat each job as it's own flow
            if (newAppsKeys.size() >= maxCount) {
              break;
            }
          FlowKey appKey = getFlowKeysFromResult(result, startTime, endTime);
          if(appKey != null) {
            newAppsKeys.add(appKey);
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
  private FlowKey getFlowKeysFromResult(Result result, Long startTime, Long endTime)
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
      if (tsl < runId) {
        runId = tsl;
      }
    }
    if((runId >= startTime) && (runId <= endTime)) {
        FlowKey fk = new FlowKey(cluster, user, appId, runId);
        return fk;
      }
    return null;
  }

}
