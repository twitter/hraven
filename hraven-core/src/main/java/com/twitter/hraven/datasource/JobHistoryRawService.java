/*
Copyright 2016 Twitter, Inc.

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobId;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.Range;
import com.twitter.hraven.util.BatchUtil;

/**
 * Used to store and retrieve {@link ProcessRecord} objects.
 */
public class JobHistoryRawService {
  private static Log LOG = LogFactory.getLog(JobHistoryRawService.class);

  private QualifiedJobIdConverter idConv = new QualifiedJobIdConverter();

  private final Connection hbaseConnection;

  /**
   * Given an HBase connection provides access to the raw table.
   * @param hbaseConnection used to create HBase table references.
   *
   * @throws IOException
   */
  public JobHistoryRawService(Connection hbaseConnection) throws IOException {
    this.hbaseConnection = hbaseConnection;
  }

  /**
   * Given a min and max jobId, get a {@link Scan} to go through all the records
   * loaded in the {@link Constants#HISTORY_RAW_TABLE}, get all the rowkeys and
   * create a list of scans with batchSize number of rows in the rawTable.
   * <p>
   * Note that this can be a somewhat slow operation as the
   * {@link Constants#HISTORY_RAW_TABLE} will have to be scanned.
   *
   * @param cluster on which the Hadoop jobs ran.
   * @param minJobId used to start the scan. If null then there is no min limit
   *          on JobId.
   * @param maxJobId used to end the scan (inclusive). If null then there is no
   *          max limit on jobId.
   * @param reprocess Reprocess those records that may have been processed
   *          already. Otherwise successfully processed jobs are skipped.
   * @param batchSize
   *
   * @return a scan of jobIds between the specified min and max. Retrieves only
   *         one version of each column.
   * @throws IOException
   * @throws RowKeyParseException when rows returned from the Raw table do not
   *           conform to the expected row key.
   */
  public List<Scan> getHistoryRawTableScans(String cluster, String minJobId,
      String maxJobId, boolean reprocess, int batchSize)
      throws IOException, RowKeyParseException {

    List<Scan> scans = new LinkedList<Scan>();

    // Get all the values in the scan so that we can evenly chop them into
    // batch size chunks.
    // The problem is that processRecords min and max can have vastly
    // overlapping ranges, and in addition, they may have a minJobId of a long
    // running Hadoop job that is processed much later. Many jobIds that are
    // of shorter jobs that have already been processed will in between the
    // min and max, but since the scan returns only the records that are not
    // already processed, the returned list may have large gaps.
    Scan scan =
        getHistoryRawTableScan(cluster, minJobId, maxJobId, reprocess, false);

    SortedSet<JobId> orderedJobIds = new TreeSet<JobId>();

    ResultScanner scanner = null;
    Table rawTable = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      LOG.info("Scanning " + Constants.HISTORY_RAW_TABLE + " table from "
          + minJobId + " to " + maxJobId);
      scanner = rawTable.getScanner(scan);
      for (Result result : scanner) {
        JobId qualifiedJobId = getQualifiedJobIdFromResult(result);
        orderedJobIds.add(qualifiedJobId);
      }
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } finally {
        if (rawTable != null) {
          rawTable.close();
        }
      }
    }

    // Now chop the set into chunks.
    List<Range<JobId>> ranges = BatchUtil.getRanges(orderedJobIds, batchSize);
    LOG.info("Dividing " + orderedJobIds.size() + " jobs in " + ranges.size()
        + " ranges.");

    for (Range<JobId> range : ranges) {
      Scan rawScan =
          getHistoryRawTableScan(cluster, range.getMin().getJobIdString(),
              range.getMax().getJobIdString(), reprocess, true);
      scans.add(rawScan);
    }

    return scans;
  }

  /**
   * Get a {@link Scan} to go through all the records loaded in the
   * {@link Constants#HISTORY_RAW_TABLE} that match the given parameters.
   *
   * @param cluster on which the Hadoop jobs ran.
   * @param minJobId used to start the scan. If null then there is no min limit
   *          on JobId.
   * @param maxJobId used to end the scan (inclusive). If null then there is no
   *          max limit on jobId.
   * @param reprocess return only those raw records that were marked to be
   *          reprocessed using
   *          {@link #markJobForReprocesssing(QualifiedJobId)}. Otherwise
   *          successfully processed jobs are skipped.
   * @param reprocess When true then reprocess argument is ignored and is
   *          assumed to be true.
   * @param includeRaw whether to include the raw column family in the scan
   *          results.
   * @return a scan of jobIds between the specified min and max. Retrieves only
   *         one version of each column.
   */
  public Scan getHistoryRawTableScan(String cluster, String minJobId,
      String maxJobId, boolean reprocess, boolean includeRaw) {
    Scan scan = new Scan();

    LOG.info("Creating scan for cluster: " + cluster);

    // Add the columns to be pulled back by this scan.
    scan.addFamily(Constants.INFO_FAM_BYTES);
    if (includeRaw) {
      scan.addFamily(Constants.RAW_FAM_BYTES);
    }

    // Pull data only for our cluster
    byte[] clusterPrefix = Bytes.toBytes(cluster + Constants.SEP);
    byte[] startRow;
    if (minJobId == null) {
      startRow = clusterPrefix;
    } else {
      startRow = idConv.toBytes(new QualifiedJobId(cluster, minJobId));
    }
    scan.setStartRow(startRow);

    LOG.info("Starting raw table scan at " + Bytes.toStringBinary(startRow)
        + " " + idConv.fromBytes(startRow));

    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // Scan only those raw rows for the specified cluster.
    PrefixFilter prefixFilter = new PrefixFilter(clusterPrefix);
    filters.addFilter(prefixFilter);

    byte[] stopRow;
    // Set to stop the scan once the last row is encountered.
    if (maxJobId != null) {
      // The inclusive stop filter actually is the accurate representation of
      // what needs to be in the result.
      byte[] lastRow = idConv.toBytes(new QualifiedJobId(cluster, maxJobId));
      InclusiveStopFilter inclusiveStopFilter =
          new InclusiveStopFilter(lastRow);
      filters.addFilter(inclusiveStopFilter);
      LOG.info("Stopping raw table scan (stop filter) at "
          + Bytes.toStringBinary(lastRow) + " " + idConv.fromBytes(lastRow));

      // Add one to the jobSequence of the maximum JobId.
      JobId maximumJobId = new JobId(maxJobId);
      JobId oneBiggerThanMaxJobId = new JobId(maximumJobId.getJobEpoch(),
          maximumJobId.getJobSequence() + 1);
      stopRow =
          idConv.toBytes(new QualifiedJobId(cluster, oneBiggerThanMaxJobId));

    } else {
      char oneBiggerSep = (char) (Constants.SEP_CHAR + 1);
      stopRow = Bytes.toBytes(cluster + oneBiggerSep);
    }
    // In addition to InclusiveStopRowFilter, set an estimated end-row that is
    // guaranteed to be bigger than the last row we want (but may over-shoot a
    // little). This helps the TableInput format limit the number of regions
    // (-servers) that need to be targeted for this scan.
    scan.setStopRow(stopRow);
    LOG.info("Stopping raw table scan (stop row) at "
        + Bytes.toStringBinary(stopRow));

    scan.setFilter(filters);

    if (reprocess) {
      SingleColumnValueExcludeFilter columnValueFilter =
          new SingleColumnValueExcludeFilter(Constants.INFO_FAM_BYTES,
              Constants.RAW_COL_REPROCESS_BYTES, CompareFilter.CompareOp.EQUAL,
              Bytes.toBytes(true));
      columnValueFilter.setFilterIfMissing(true);
      filters.addFilter(columnValueFilter);
    } else {
      // Process each row only once. If it is already processed, then do not do
      // it again.
      SingleColumnValueExcludeFilter columnValueFilter =
          new SingleColumnValueExcludeFilter(Constants.INFO_FAM_BYTES,
              Constants.JOB_PROCESSED_SUCCESS_COL_BYTES,
              CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true));
      filters.addFilter(columnValueFilter);
    }

    /*
     * Create a filter that passes only if both the jobconf and job history
     * blobs are present. Here we use the last mod time columns as a proxy for
     * their existence, since the byte comparison is much cheaper (requires an
     * array copy). We do not want to process rows where we don't have both.
     */
    byte[] empty = Bytes.toBytes(0L);
    FilterList bothColumnFilters =
        new FilterList(FilterList.Operator.MUST_PASS_ALL);
    SingleColumnValueFilter jobConfFilter = new SingleColumnValueFilter(
        Constants.INFO_FAM_BYTES, Constants.JOBCONF_LAST_MODIFIED_COL_BYTES,
        CompareFilter.CompareOp.GREATER, empty);
    jobConfFilter.setFilterIfMissing(true);
    bothColumnFilters.addFilter(jobConfFilter);
    SingleColumnValueFilter jobHistoryFilter = new SingleColumnValueFilter(
        Constants.INFO_FAM_BYTES, Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES,
        CompareFilter.CompareOp.GREATER, empty);
    jobHistoryFilter.setFilterIfMissing(true);
    bothColumnFilters.addFilter(jobHistoryFilter);

    filters.addFilter(bothColumnFilters);

    scan.setFilter(filters);

    // Let's be nice; we are reading potentially large amounts of data that
    // could take a bit to process.
    scan.setCacheBlocks(false);
    scan.setCaching(1);

    scan.setMaxVersions(1);

    return scan;
  }

  /**
   * Returns the raw job configuration stored for the given cluster and job ID
   * @param jobId the cluster and job ID to look up
   * @return the stored job configuration
   * @throws IOException
   */
  public Configuration getRawJobConfiguration(QualifiedJobId jobId)
      throws IOException {
    Configuration conf = null;
    byte[] rowKey = idConv.toBytes(jobId);
    Get get = new Get(rowKey);
    get.addColumn(Constants.RAW_FAM_BYTES, Constants.JOBCONF_COL_BYTES);
    Table rawTable = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      Result result = rawTable.get(get);
      if (result != null && !result.isEmpty()) {
        conf = createConfigurationFromResult(result);
      }
    } catch (MissingColumnInResultException e) {
      LOG.error(
          "Failed to retrieve configuration from row returned for " + jobId, e);
    } finally {
      if (rawTable != null) {
        rawTable.close();
      }
    }
    return conf;
  }

  /**
   * Returns the raw job history file stored for the given cluster and job ID.
   * @param jobId the cluster and job ID to look up
   * @return the stored job history file contents or {@code null} if no
   *         corresponding record was found
   * @throws IOException
   */
  public String getRawJobHistory(QualifiedJobId jobId) throws IOException {
    String historyData = null;
    byte[] rowKey = idConv.toBytes(jobId);
    Get get = new Get(rowKey);
    get.addColumn(Constants.RAW_FAM_BYTES, Constants.JOBHISTORY_COL_BYTES);
    Table rawTable = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      Result result = rawTable.get(get);
      if (result != null && !result.isEmpty()) {
        historyData = Bytes.toString(result.getValue(Constants.RAW_FAM_BYTES,
            Constants.JOBHISTORY_COL_BYTES));
      }
    } finally {
      if (rawTable != null) {
        rawTable.close();
      }
    }

    return historyData;
  }

  /**
   * Returns the raw job history file as a byte array stored for the given
   * cluster and job ID.
   * @param jobId the cluster and job ID to look up
   * @return the stored job history file contents or {@code null} if no
   *         corresponding record was found
   * @throws IOException
   */
  public byte[] getRawJobHistoryBytes(QualifiedJobId jobId) throws IOException {
    byte[] historyData = null;
    byte[] rowKey = idConv.toBytes(jobId);
    Get get = new Get(rowKey);
    get.addColumn(Constants.RAW_FAM_BYTES, Constants.JOBHISTORY_COL_BYTES);
    Table rawTable = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      Result result = rawTable.get(get);
      if (result != null && !result.isEmpty()) {
        historyData = result.getValue(Constants.RAW_FAM_BYTES,
            Constants.JOBHISTORY_COL_BYTES);
      }
    } finally {
      if (rawTable != null) {
        rawTable.close();
      }
    }
    return historyData;
  }

  /**
   * @param result from the {@link Scan} from
   *          {@link getHistoryRawTableScan(String, String, String, boolean,
   *          boolean, boolean)}
   * @return the configuration part.
   * @throws MissingColumnInResultException when the result does not contain
   *           {@link Constants#RAW_FAM}, {@link Constants#JOBCONF_COL}.
   */
  public Configuration createConfigurationFromResult(Result result)
      throws MissingColumnInResultException {

    if (result == null) {
      throw new IllegalArgumentException("Cannot create InputStream from null");
    }

    Cell cell = result.getColumnLatestCell(Constants.RAW_FAM_BYTES,
        Constants.JOBCONF_COL_BYTES);

    // Create a jobConf from the raw input
    Configuration jobConf = new Configuration(false);

    byte[] jobConfRawBytes = null;
    if (cell != null) {
      jobConfRawBytes = CellUtil.cloneValue(cell);
    }
    if (jobConfRawBytes == null || jobConfRawBytes.length == 0) {
      throw new MissingColumnInResultException(Constants.RAW_FAM_BYTES,
          Constants.JOBCONF_COL_BYTES);
    }

    InputStream in = new ByteArrayInputStream(jobConfRawBytes);
    jobConf.addResource(in);

    // Configuration property loading is lazy, so we need to force a load from
    // the input stream
    try {
      int size = jobConf.size();
      if (LOG.isDebugEnabled()) {
        LOG.info(
            "Loaded " + size + " job configuration properties from result");
      }
    } catch (Exception e) {
      throw new ProcessingException("Invalid configuration from result "
          + Bytes.toStringBinary(result.getRow()), e);
    }

    return jobConf;
  }

  /**
   * @param cluster the identifier for the Hadoop cluster on which a job ran
   * @param jobId the identifier of the job as run on the JobTracker.
   * @return the rowKey used in the JobHistory Raw table.
   */
  public byte[] getRowKey(String cluster, String jobId) {
    return idConv.toBytes(new QualifiedJobId(cluster, jobId));
  }

  /**
   * Given a result from the {@link Scan} obtained by
   * {@link getHistoryRawTableScan(String, String, String, boolean, boolean,
   * boolean)} . They for puts into the raw table are constructed using
   * {@link #getRowKey(String, String)}
   *
   * @param result from which to pull the jobKey
   * @return the qualified job identifier
   * @throws RowKeyParseException if the rowkey cannot be parsed properly
   */
  public QualifiedJobId getQualifiedJobIdFromResult(Result result)
      throws RowKeyParseException {

    if (result == null) {
      throw new RowKeyParseException(
          "Cannot parse empty row key from result in HBase table: "
              + Constants.HISTORY_RAW_TABLE);
    }
    return idConv.fromBytes(result.getRow());
  }

  /**
   * @param result from the {@link Scan} from
   *          {@link #getHistoryRawTableScan(String, String, String, boolean, boolean, boolean)}
   *          this cannot be null;
   * @return an inputStream from the JobHistory
   * @throws MissingColumnInResultException when the result does not contain
   *           {@link Constants#RAW_FAM}, {@link Constants#JOBHISTORY_COL}.
   */
  public InputStream getJobHistoryInputStreamFromResult(Result result)
      throws MissingColumnInResultException {

    InputStream is =
        new ByteArrayInputStream(getJobHistoryRawFromResult(result));
    return is;
  }

  /**
   * @param row the identifier of the row in the RAW table. Cannot be null.
   * @success whether the job was processed successfully or not
   * @return a put to indicate that this job has been processed successfully.
   */
  public Put getJobProcessedSuccessPut(byte[] row, boolean success) {
    Put put = new Put(row);
    put.addColumn(Constants.INFO_FAM_BYTES,
        Constants.JOB_PROCESSED_SUCCESS_COL_BYTES, Bytes.toBytes(success));
    if (success) {
      // Make sure we mark that this row does not have to be reloaded, no matter
      // if it is the first time, or it was marked with reload before.
      put.addColumn(Constants.INFO_FAM_BYTES, Constants.RAW_COL_REPROCESS_BYTES,
          Bytes.toBytes(false));
    }
    return put;
  }

  /**
   * attempts to approximately set the job submit time based on the last
   * modification time of the job history file
   * @param value result
   * @return approximate job submit time
   * @throws MissingColumnInResultException
   */
  public long getApproxSubmitTime(Result value)
      throws MissingColumnInResultException {
    if (value == null) {
      throw new IllegalArgumentException(
          "Cannot get last modification time from " + "a null hbase result");
    }

    Cell cell = value.getColumnLatestCell(Constants.INFO_FAM_BYTES,
        Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES);

    if (cell == null) {
      throw new MissingColumnInResultException(Constants.INFO_FAM_BYTES,
          Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES);
    }

    byte[] lastModTimeBytes = CellUtil.cloneValue(cell);
    // we try to approximately set the job submit time based on when the job
    // history file
    // was last modified and an average job duration
    long lastModTime = Bytes.toLong(lastModTimeBytes);
    long jobSubmitTimeMillis = lastModTime - Constants.AVERGAE_JOB_DURATION;
    LOG.debug("Approximate job submit time is " + jobSubmitTimeMillis
        + " based on " + lastModTime);
    return jobSubmitTimeMillis;
  }

  /**
   * @param row the identifier for the row in the RAW table. Cannot be null.
   * @param submitTimeMillis
   * @return
   */
  public Put getJobSubmitTimePut(byte[] row, long submitTimeMillis) {
    Put put = new Put(row);
    put.addColumn(Constants.INFO_FAM_BYTES, Constants.SUBMIT_TIME_COL_BYTES,
        Bytes.toBytes(submitTimeMillis));
    return put;
  }

  /**
   * Flags a job's RAW record for reprocessing
   *
   * @param jobId
   */
  public void markJobForReprocesssing(QualifiedJobId jobId) throws IOException {
    Put p = new Put(idConv.toBytes(jobId));
    p.addColumn(Constants.INFO_FAM_BYTES, Constants.RAW_COL_REPROCESS_BYTES,
        Bytes.toBytes(true));
    Table rawTable = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      rawTable.put(p);
    } finally {
      if (rawTable != null) {
        rawTable.close();
      }
    }
  }

  /**
   * returns the raw byte representation of job history from the result value
   * @param value result
   * @return byte array of job history raw
   *
   * @throws MissingColumnInResultException
   */
  public byte[] getJobHistoryRawFromResult(Result value)
      throws MissingColumnInResultException {
    if (value == null) {
      throw new IllegalArgumentException("Cannot create InputStream from null");
    }

    Cell cell = value.getColumnLatestCell(Constants.RAW_FAM_BYTES,
        Constants.JOBHISTORY_COL_BYTES);

    // Could be that there is no conf file (only a history file).
    if (cell == null) {
      throw new MissingColumnInResultException(Constants.RAW_FAM_BYTES,
          Constants.JOBHISTORY_COL_BYTES);
    }

    byte[] jobHistoryRaw = CellUtil.cloneValue(cell);
    return jobHistoryRaw;
  }

  /**
   * creates a put to be updated into the RAW table for aggregation status
   * @param row key
   * @param status of aggregation
   * @return {@link Put}
   */
  public Put getAggregatedStatusPut(byte[] row, byte[] col, Boolean status) {
    Put put = new Put(row);
    put.addColumn(Constants.INFO_FAM_BYTES, col, Bytes.toBytes(status));
    try {
      LOG.info(" agg status " + status + " and put " + put.toJSON());
    } catch (IOException e) {
      // ignore json exception
    }
    return put;
  }

  /**
   * creates a Get to be fetch daily aggregation status from the RAW table
   * @param row key
   * @return {@link Get}
   * @throws IOException
   */
  public boolean getStatusAgg(byte[] row, byte[] col) throws IOException {
    Get g = new Get(row);
    g.addColumn(Constants.INFO_FAM_BYTES, col);
    Table rawTable = null;
    Cell cell = null;
    try {
      rawTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.HISTORY_RAW_TABLE));
      Result r = rawTable.get(g);
      cell = r.getColumnLatestCell(Constants.INFO_FAM_BYTES, col);
    } finally {
      if (rawTable != null) {
        rawTable.close();
      }
    }
    boolean status = false;
    try {
      if (cell != null) {
        status = Bytes.toBoolean(CellUtil.cloneValue(cell));
      }
    } catch (IllegalArgumentException iae) {
      LOG.error("Caught " + iae);
    }
    LOG.info("Returning from Raw, " + Bytes.toString(col) + " for this job="
        + status);
    return status;
  }
}
