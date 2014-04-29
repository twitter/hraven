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
import com.twitter.hraven.CapacityDetails;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
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
   * looks up those jobs in the job history table to fetch their flow details
   * populates the capacity numbers for these jobs
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
  public List<Flow> getNewJobs(String cluster, String user, Long startTime,
      Long endTime, int limit, Map<String, CapacityDetails> queueCapacity, JobHistoryService jhs)
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
    LOG.info(" queue capacity size " + queueCapacity.size());

    List<Flow> newJobs = new ArrayList<Flow>();
      try {
        newJobs = createFromResults(scan, startTime, endTime, limit, jhs, queueCapacity);
      } catch (IOException e) {
        LOG.error("Caught exception while trying to scan, returning empty list of flows: " + e.toString());
      }

    return newJobs;
  }

  private List<Flow> createFromResults(Scan scan, Long startTime, Long endTime, int maxCount,
      JobHistoryService jhs, Map<String, CapacityDetails> queueCapacity)
          throws IOException {
    ResultScanner scanner = null;
    List<Flow> newJobs = new ArrayList<Flow>();
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
            if (newJobs.size() >= maxCount) {
              break;
            }
          Flow job = getFlowDescFromResult(result, startTime, endTime, jhs, queueCapacity);
          if(job != null) {
            newJobs.add(job);
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

    return newJobs;
  }

  private Flow getFlowDescFromResult(Result result, Long startTime, Long endTime,
      JobHistoryService jhs, Map<String, CapacityDetails> queueCapacity) throws IOException {

    byte[] rowKey = result.getRow();
    byte[][] keyComponents = ByteUtil.split(rowKey, Constants.SEP_BYTES);
    String cluster = Bytes.toString(keyComponents[0]);
    String user = Bytes.toString(keyComponents[1]);
    String appId = Bytes.toString(keyComponents[2]);

    NavigableMap<byte[],byte[]> valueMap = result.getFamilyMap(Constants.INFO_FAM_BYTES);
    Long runId = Long.MAX_VALUE;
    String minVersion = null;
    for (Map.Entry<byte[],byte[]> entry : valueMap.entrySet()) {
      Long tsl = Bytes.toLong(entry.getValue()) ;
      if (tsl < runId) {
        runId = tsl;
        minVersion = Bytes.toString(entry.getKey());
      }
    }
    if((runId >= startTime) && (runId <= endTime)) {
      if ( jhs == null ) {
        FlowKey fk = new FlowKey(cluster, user, appId, runId);
        Flow f = new Flow(fk);
        f.setVersion(minVersion);
        return f;
      }
      Flow flow = jhs.getFlow(cluster, user, appId, runId, Boolean.FALSE);
      if (flow == null) {
        // this flow should normally not be null
        // but there was a bug where the runId was not being set correctly,
        // hence in this step, make a best effort attempt to retrieve SOME data
        // so we try to get the latest flow with this version here
        LOG.info("Could not find flow with run Id " + runId + " for " + cluster + " " + user + " "
            + appId + " fetching flowSeries with version " + minVersion);
        List<Flow> fl = jhs.getFlowSeries(cluster, user, appId, minVersion, false, 1);
        if (fl.size() > 0) {
          flow = fl.get(0);
        }
      }
      if (flow == null) {
        // this code probably never gets executed, but
        // for whatever reason (potential bug elsewhere),
        // if no flow was found in the steps above,
        // we have the flow key parameters, hence we create an empty flow with
        // these entries from the data in appVersion table
        LOG.info("Could not find flow series with version " + minVersion + " for " + cluster + " " + user + " "
            + appId );
        FlowKey fk = new FlowKey(cluster, user, appId, runId);
        flow = new Flow(fk);
      }
      flow.setVersion(minVersion);
      // get the pool capacity
      String queue = flow.getQueue();
      if (queue == null) {
        queue = user;
      }
      CapacityDetails cd = queueCapacity.get(queue);
      if(cd != null) {
        flow.setQueueMinResources(cd.getMinResources());
        flow.setQueueMinMaps(cd.getMinMaps());
        flow.setQueueMinReduces(cd.getMinReduces());
      } else {
        LOG.info("No capacity details found for " + queue);
      }
      LOG.info(" queue: " + queue + " " + flow.getAppId() + " " + flow.getCluster() + " "
          + flow.getUserName() + " " + flow.getQueueMinResources() + " " + flow.getQueueMinMaps()
          + " " + flow.getQueueMinReduces());
      return flow;
    }
    return null;
 }

}
