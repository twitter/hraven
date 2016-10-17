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
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.twitter.hraven.Constants;

/**
 * Reads and writes information about the mapping of application IDs
 * to version numbers.
 */
public class AppVersionService {
  private static Log LOG = LogFactory.getLog(AppVersionService.class);

  @SuppressWarnings("unused")
  private final Configuration conf;
  private final Connection conn;
  private final Table versionsTable;

  /**
   * Opens a new connection to HBase server and opens connections to the tables.
   *
   * User is responsible for calling {@link #close()} when finished using this service.
   *
   * @param hbaseConf
   *          configuration of the processing job, not the conf of the files we
   *          are processing. Used to connect to HBase.
   * @throws IOException
   */
  public AppVersionService(Configuration hbaseConf) throws IOException {
    if (hbaseConf == null) {
      conf = new Configuration();
    } else {
      conf = hbaseConf;
    }

    conn = ConnectionFactory.createConnection(conf);

    versionsTable = conn.getTable(TableName.valueOf(Constants.HISTORY_APP_VERSION_TABLE));
  }

  /**
   * close open connections to tables and the hbase cluster.
   * @throws IOException
   */
  public void close() throws IOException {
    IOException ret = null;

    try {
      if (versionsTable != null) {
        versionsTable.close();
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      ret = ioe;
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      ret = ioe;
    }

    if (ret != null) {
      throw ret;
    }
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
      for (Cell c : r.listCells()) {
        versions.add(
            new VersionInfo(
                Bytes.toString(CellUtil.cloneQualifier(c)),
                Bytes.toLong(CellUtil.cloneValue(c))));
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
      for (Cell c : r.listCells()) {
        ts = 0L;
        try {
          ts = Bytes.toLong(CellUtil.cloneValue(c));
          versions.add(
              new VersionInfo(Bytes.toString(CellUtil.cloneQualifier(c)), ts) );
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

  private byte[] getRowKey(String cluster, String user, String appId) {
    String keyString = new StringBuilder(cluster)
        .append(Constants.SEP).append(user)
        .append(Constants.SEP).append(appId).toString();
    return Bytes.toBytes(keyString);
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return versionsTable.getScanner(scan);
  }

}
