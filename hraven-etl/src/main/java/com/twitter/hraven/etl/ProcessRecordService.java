/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.hraven.etl;

import static com.twitter.hraven.etl.ProcessState.CREATED;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.NOT_EQUAL;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.NO_OP;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.etl.JobFile;
import com.twitter.hraven.etl.ProcessRecord;
import com.twitter.hraven.etl.ProcessRecordKey;
import com.twitter.hraven.etl.ProcessState;

/**
 * Used to store and retrieve {@link ProcessRecord} objects.
 * 
 */
public class ProcessRecordService {
  /**
   * Used to get the end of the day in millis like so yyyy-MM-dd HH:mm:ss.SSS
   */
  public static final SimpleDateFormat MILLISECOND_TIMSTAMP_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.SSS");

  // Initialize to use UTC
  static {
    TimeZone utc = TimeZone.getTimeZone("UTC");
    MILLISECOND_TIMSTAMP_FORMAT.setTimeZone(utc);
  }

  private static Log LOG = LogFactory.getLog(ProcessRecordService.class);

  private ProcessRecordKeyConverter keyConv = new ProcessRecordKeyConverter();

  /**
   * Used to store the processRecords in HBase
   */
  private final HTable processRecordTable;

  /**
   * Used to access the filesystem.
   */
  private final Configuration myHBaseConf;

  private final FileSystem fs;

  /**
   * Constructor. Note that caller is responsible to {@link #close()} created
   * instances.
   * 
   * @param myHBaseConf
   *          configuration of the processing job, not the conf of the files we
   *          are processing. This should be an HBase conf so that we can access
   *          the appropriate cluster.
   * @throws IOException
   *           in case we have problems connecting to HBase.
   */
  public ProcessRecordService(Configuration myHBaseConf) throws IOException {
    processRecordTable = new HTable(myHBaseConf,
        Constants.JOB_FILE_PROCESS_TABLE_BYTES);
    this.myHBaseConf = myHBaseConf;
    fs = FileSystem.get(myHBaseConf);
  }

  /**
   * Write all fields of a record to HBase. To be used in initial insert, or to
   * overwrite whatever values are there in HBase.
   * <p>
   * Consider using {@link #setProcessState(ProcessRecord, ProcessState)} if you
   * want to update only the state.
   * 
   * @param processRecord
   *          non-null ProcessRecord to write to HBase.
   * @throws IOException
   *           if the record cannot be written.
   */
  public void writeJobRecord(ProcessRecord processRecord) throws IOException {

    byte[] key = keyConv.toBytes(processRecord.getKey());
    Put put = new Put(key);

    put.add(Constants.INFO_FAM_BYTES,
        Constants.MIN_MOD_TIME_MILLIS_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getMinModificationTimeMillis()));
    put.add(Constants.INFO_FAM_BYTES,
        Constants.PROCESSED_JOB_FILES_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getProcessedJobFiles()));
    put.add(Constants.INFO_FAM_BYTES, Constants.PROCESS_FILE_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getProcessFile()));
    put.add(Constants.INFO_FAM_BYTES, Constants.PROCESSING_STATE_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getProcessState().getCode()));
    put.add(Constants.INFO_FAM_BYTES, Constants.MIN_JOB_ID_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getMinJobId()));
    put.add(Constants.INFO_FAM_BYTES, Constants.MAX_JOB_ID_COLUMN_BYTES,
        Bytes.toBytes(processRecord.getMaxJobId()));

    processRecordTable.put(put);
  }

  /**
   * @param cluster
   *          for which to return the last ProcessRecord.
   * @return the last process record that is not in {@link ProcessState#CREATED}
   *         state.
   * @throws IOException
   */
  public ProcessRecord getLastSuccessfulProcessRecord(String cluster, String processFileSubstring)
      throws IOException {
    List<ProcessRecord> processRecords = getProcessRecords(cluster, NOT_EQUAL,
        CREATED, 1, processFileSubstring);
    if (processRecords.size() > 0) {
      return processRecords.get(0);
    }
    // Did not get any record.
    return null;
  }

  /**
   * @param cluster
   *          for which to return the last ProcessRecord.
   * @param maxCount
   *          the maximum number of results to return.
   * @param processFileSubstring
   *          return rows where the process file path contains this string. If
   *          <code>null</code> or empty string, then no filtering is applied.
   * @return the last process record that is not in {@link ProcessState#CREATED}
   *         state. Note that no records with a maxModificationTime of 0
   *         (beginning of time) will be returned
   * @throws IOException
   */
  public List<ProcessRecord> getProcessRecords(String cluster, int maxCount,
      String processFileSubstring) throws IOException {
    return getProcessRecords(cluster, NO_OP, null, maxCount,
        processFileSubstring);
  }

  /**
   * @param cluster
   *          for which to return the last ProcessRecord.
   * @param processState
   *          return only rows with this state
   * @param maxCount
   *          the maximum number of results to return.
   * @param processFileSubstring
   *          return rows where the process file path contains this string. If
   *          <code>null</code> or empty string, then no filtering is applied.
   * @return the last process record that is not in {@link ProcessState#CREATED}
   *         state. Note that no records with a maxModificationTime of 0
   *         (beginning of time) will be returned
   * @throws IOException
   */
  public List<ProcessRecord> getProcessRecords(String cluster,
      ProcessState processState, int maxCount, String processFileSubstring)
      throws IOException {
    return getProcessRecords(cluster, EQUAL, processState, maxCount,
        processFileSubstring);
  }

  /**
   * @param cluster
   *          for which to return the last ProcessRecord.
   * @param compareOp
   *          to apply to the processState argument. If {@link CompareOp#NO_OP}
   *          is passed, then no filter is used at all, and processState
   *          argument is ignored.
   * @param processState
   *          return rows where the compareOp applies.
   * @param maxCount
   *          the maximum number of results to return.
   * @param processFileSubstring
   *          return rows where the process file path contains this string. If
   *          <code>null</code> or empty string, then no filtering is applied.
   * @return the last process record that is not in {@link ProcessState#CREATED}
   *         state. Note that no records with a maxModificationTime of 0
   *         (beginning of time) will be returned
   * @throws IOException
   */
  public List<ProcessRecord> getProcessRecords(String cluster,
      CompareOp compareOp, ProcessState processState, int maxCount,
      String processFileSubstring) throws IOException {
    Scan scan = new Scan();
    // Pull data only for our cluster
    scan.setStartRow(keyConv.toBytes(new ProcessRecordKey(cluster,
        Long.MAX_VALUE)));
    // Records are sorted in reverse order, so the last one for this cluster
    // would be the one with a modification time at the beginning of time.
    scan.setStopRow(keyConv.toBytes(new ProcessRecordKey(cluster, 0)));

    scan.addColumn(Constants.INFO_FAM_BYTES,
        Constants.MIN_MOD_TIME_MILLIS_COLUMN_BYTES);
    scan.addColumn(Constants.INFO_FAM_BYTES,
        Constants.PROCESSED_JOB_FILES_COLUMN_BYTES);
    scan.addColumn(Constants.INFO_FAM_BYTES,
        Constants.PROCESS_FILE_COLUMN_BYTES);
    scan.addColumn(Constants.INFO_FAM_BYTES,
        Constants.PROCESSING_STATE_COLUMN_BYTES);
    scan.addColumn(Constants.INFO_FAM_BYTES, Constants.MIN_JOB_ID_COLUMN_BYTES);
    scan.addColumn(Constants.INFO_FAM_BYTES, Constants.MAX_JOB_ID_COLUMN_BYTES);
    scan.setMaxVersions(1);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // Filter on process state only when needed.
    if (!NO_OP.equals(compareOp)) {
      byte[] filterColumnValue = Bytes.toBytes(processState.getCode());
      Filter processingStatefilter = new SingleColumnValueFilter(
          Constants.INFO_FAM_BYTES, Constants.PROCESSING_STATE_COLUMN_BYTES,
          compareOp, filterColumnValue);
      filterList.addFilter(processingStatefilter);
    }

    // Filter on process file only when needed
    if (processFileSubstring != null && processFileSubstring.length() > 0) {
      SubstringComparator ssc = new SubstringComparator(processFileSubstring);
      Filter processFileFilter = new SingleColumnValueFilter(
          Constants.INFO_FAM_BYTES, Constants.PROCESS_FILE_COLUMN_BYTES, EQUAL,
          ssc);
      filterList.addFilter(processFileFilter);
    }

    // Add filters only if any filter was actually needed.
    if (filterList.getFilters().size() > 0) {
      scan.setFilter(filterList);
    }

    ResultScanner scanner = null;

    List<ProcessRecord> records = null;
    try {
      scanner = processRecordTable.getScanner(scan);
      records = createFromResults(scanner, maxCount);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    return records;
  }

  /**
   * Transform results pulled from a scanner and turn into a list of
   * ProcessRecords.
   * 
   * @param scanner
   *          used to pull the results from, in the order determined by the
   *          scanner.
   * @param maxCount
   *          maximum number of results to return.
   * @return
   */
  private List<ProcessRecord> createFromResults(ResultScanner scanner,
      int maxCount) {
    // Defensive coding
    if ((maxCount <= 0) || (scanner == null)) {
      return new ArrayList<ProcessRecord>(0);
    }
    List<ProcessRecord> records = new ArrayList<ProcessRecord>();

    for (Result result : scanner) {
      byte[] row = result.getRow();
      ProcessRecordKey key = keyConv.fromBytes(row);

      KeyValue keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.MIN_MOD_TIME_MILLIS_COLUMN_BYTES);
      long minModificationTimeMillis = Bytes.toLong(keyValue.getValue());

      keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.PROCESSED_JOB_FILES_COLUMN_BYTES);
      int processedJobFiles = Bytes.toInt(keyValue.getValue());

      keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.PROCESS_FILE_COLUMN_BYTES);
      String processingDirectory = Bytes.toString(keyValue.getValue());

      keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.PROCESSING_STATE_COLUMN_BYTES);
      ProcessState processState = ProcessState.getProcessState(Bytes
          .toInt(keyValue.getValue()));

      keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.MIN_JOB_ID_COLUMN_BYTES);
      String minJobId = null;
      if (keyValue != null) {
        minJobId = Bytes.toString(keyValue.getValue());
      }

      keyValue = result.getColumnLatest(Constants.INFO_FAM_BYTES,
          Constants.MAX_JOB_ID_COLUMN_BYTES);
      String maxJobId = null;
      if (keyValue != null) {
        maxJobId = Bytes.toString(keyValue.getValue());
      }

      ProcessRecord processRecord = new ProcessRecord(key.getCluster(),
          processState, minModificationTimeMillis, key.getTimestamp(),
          processedJobFiles, processingDirectory, minJobId, maxJobId);
      records.add(processRecord);

      // Check if we retrieved enough records.
      if (records.size() >= maxCount) {
        break;
      }
    }

    LOG.info("Returning " + records.size() + " process records");

    return records;
  }

  /**
   * Set the process state for a given processRecord.
   * 
   * @param processRecord
   *          for which to update the state
   * @param newState
   *          the new state to set in HBase.
   * @return a new ProcessRecord with the new state.
   * @throws IOException
   */
  public ProcessRecord setProcessState(ProcessRecord processRecord,
      ProcessState newState) throws IOException {
    Put put = new Put(keyConv.toBytes(processRecord.getKey()));
    put.add(Constants.INFO_FAM_BYTES, Constants.PROCESSING_STATE_COLUMN_BYTES,
        Bytes.toBytes(newState.getCode()));
    processRecordTable.put(put);
    ProcessRecord updatedProcessRecord = new ProcessRecord(
        processRecord.getCluster(), newState,
        processRecord.getMinModificationTimeMillis(),
        processRecord.getMaxModificationTimeMillis(),
        processRecord.getProcessedJobFiles(), processRecord.getProcessFile(),
        processRecord.getMinJobId(), processRecord.getMaxJobId());
    return updatedProcessRecord;
  }

  /**
   * @param year
   *          the year in 4 characters like "2012"
   * @param month
   *          the month in 2 characters like "05"
   * @param day
   *          the day in 2 characters like "08"
   * @return End of the day in milliseconds since January 1, 1970 UTC
   *         (including)
   */
  long getEndOfDayMillis(String year, String month, String day) {
    // Assemble string in this format: yyyy-MM-dd HH:mm:ss.SSS
    String endOfDay = year + "-" + month + "-" + day + " 23:59:59.999";
    try {
      Date endOfDayDate = MILLISECOND_TIMSTAMP_FORMAT.parse(endOfDay);
      return endOfDayDate.getTime();
    } catch (java.text.ParseException e) {
      throw new IllegalArgumentException("Cannot parse: " + endOfDay);
    }
  }

  /**
   * @param year
   *          the year in 4 characters like "2012"
   * @param month
   *          the month in 2 characters like "05"
   * @param day
   *          the day in 2 characters like "08"
   * @return Start of the day in milliseconds since January 1, 1970 UTC
   *         (including)
   */
  long getStartOfDayMillis(String year, String month, String day) {
    // Assemble string in this format: yyyy-MM-dd HH:mm:ss.SSS
    String startOfDay = year + "-" + month + "-" + day + " 00:00:00.000";
    try {
      Date startOfDayDate = MILLISECOND_TIMSTAMP_FORMAT.parse(startOfDay);
      return startOfDayDate.getTime();
    } catch (java.text.ParseException e) {
      throw new IllegalArgumentException("Cannot parse: " + startOfDay);
    }
  }

  /**
   * Release internal HBase table instances. Must be called when consumer is
   * done with this service.
   * 
   * @throws IOException
   *           when bad things happen closing HBase table(s).
   */
  public void close() throws IOException {
    if (processRecordTable != null) {
      processRecordTable.close();
    }
  }

  /**
   * @param cluster
   *          the cluster on which the batch of jobs ran.
   * @param batch
   *          indicating which batch this is. Used to make the filename unique.
   * @return Path to a processFile in the /tmp directory on the filesystem.
   */
  public Path getInitialProcessFile(String cluster, int batch) {
    long now = System.currentTimeMillis();
    String timestamp = Constants.TIMESTAMP_FORMAT.format(new Date(now));

    String safeCluster = "";
    if (cluster != null) {
      // rip out everything that is not letter, number or underscore.
      safeCluster = cluster.replaceAll("\\W+", "");
    }

    String processFileName = Constants.PROJECT_NAME + "-" + safeCluster + "-"
        + timestamp + "-" + batch;
    Path tmpDir = new Path("/tmp");
    Path processFile = new Path(tmpDir, processFileName);
    return processFile;
  }

  /**
   * @param processFilePath
   *          where to write to.
   * @return Writer for SequenceFile<JobFile, FileStatus>
   * @throws IOException
   *           when bad things happen.
   */
  public Writer createProcessFileWriter(Path processFilePath)
      throws IOException {
    Writer indexWriter = SequenceFile.createWriter(fs, myHBaseConf,
        processFilePath, JobFile.class, FileStatus.class);
    return indexWriter;
  }

  /**
   * @param initialProcessFile
   *          The path to the file to be moved.
   * @param outputPath
   *          The path where this file is to be moved to.
   * @return the new path or null if the rename failed.
   * @throws IOException
   *           when bad things happen.
   * @throws ProcessingException
   *           when the file cannot be moved.
   */
  public Path moveProcessFile(Path initialProcessFile, Path outputPath)
      throws IOException {
    String processFileName = initialProcessFile.getName();
    Path processFile = new Path(outputPath, processFileName);

    boolean success = fs.rename(initialProcessFile, processFile);
    if (!success) {
      throw new ProcessingException("Unable to move processing file "
          + initialProcessFile + " to " + processFile);
    }
    return processFile;
  }
}
