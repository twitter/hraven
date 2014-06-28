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
package com.twitter.hraven.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.*;
import com.twitter.hraven.util.ByteUtil;
import com.twitter.hraven.util.HadoopConfUtil;

/**
 */
public class JobHistoryService {
  private static Log LOG = LogFactory.getLog(JobHistoryService.class);

  private final Configuration myConf;
  private final HTable historyTable;
  private final HTable taskTable;
  private final JobHistoryByIdService idService;
  private final JobKeyConverter jobKeyConv = new JobKeyConverter();
  private final TaskKeyConverter taskKeyConv = new TaskKeyConverter();

  private final int defaultScannerCaching;

  public JobHistoryService(Configuration myConf) throws IOException {
    this.myConf = myConf;
    this.historyTable = new HTable(myConf, Constants.HISTORY_TABLE_BYTES);
    this.taskTable = new HTable(myConf, Constants.HISTORY_TASK_TABLE_BYTES);
    this.idService = new JobHistoryByIdService(this.myConf);
    this.defaultScannerCaching = myConf.getInt("hbase.client.scanner.caching", 100);
  }

  /**
   * Returns the most recent flow by application ID. This version will only
   * populate job-level details not task information. To include task details
   * use
   * {@link JobHistoryService#getLatestFlow(String, String, String, boolean)}.
   * 
   * @param cluster the cluster identifier
   * @param user the user running the jobs
   * @param appId the application description
   * @return
   */
  public Flow getLatestFlow(String cluster, String user, String appId)
      throws IOException {
    return getLatestFlow(cluster, user, appId, false);
  }

  /**
   * Returns the most recent flow by application ID. This version will populate
   * both job-level for all jobs in the flow, and task-level data for each job.
   * 
   * @param cluster the cluster identifier
   * @param user the user running the jobs
   * @param appId the application description
   * @return
   */
  public Flow getLatestFlow(String cluster, String user, String appId,
      boolean populateTasks) throws IOException {
    List<Flow> flows = getFlowSeries(cluster, user, appId, null, populateTasks,
        1);
    if (flows.size() > 0) {
      return flows.get(0);
    }
    return null;
  }

  /**
   * Returns up to {@code limit} most recent flows by application ID. This
   * version will only populate job-level details not task information. To
   * include task details use
   * {@link JobHistoryService#getFlowSeries(String, String, String, String, boolean, int)}
   * .
   * 
   * @param cluster the cluster identifier
   * @param user the user running the jobs
   * @param appId the application description
   * @param limit the maximum number of Flow instances to return
   * @return
   */
  public List<Flow> getFlowSeries(String cluster, String user, String appId,
      int limit) throws IOException {
    return getFlowSeries(cluster, user, appId, null, false, limit);
  }

  /**
   * Returns the {@link Flow} instance matching the application ID and run ID.
   *
   * @param cluster the cluster identifier
   * @param user the user running the jobs
   * @param appId the application description
   * @param runId the specific run ID for the flow
   * @param populateTasks whether or not to populate the task details for each job
   * @return
   */
  public Flow getFlow(String cluster, String user, String appId, long runId, boolean populateTasks)
  throws IOException {
    Flow flow = null;

    byte[] startRow = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(cluster), Bytes.toBytes(user), Bytes.toBytes(appId),
        Bytes.toBytes(FlowKey.encodeRunId(runId)), Constants.EMPTY_BYTES);

    LOG.info("Reading job_history rows start at " + Bytes.toStringBinary(startRow));
    Scan scan = new Scan();
    // start scanning history at cluster!user!app!run!
    scan.setStartRow(startRow);
    // require that all results match this flow prefix
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(startRow)));

    List<Flow> flows = createFromResults(scan, populateTasks, 1);
    if (flows.size() > 0) {
      flow = flows.get(0);
    }

    return flow;
  }

  /**
   * Returns the {@link Flow} instance containing the given job ID.
   * 
   * @param cluster the cluster identifier
   * @param jobId the job identifier
   * @return
   */
  public Flow getFlowByJobID(String cluster, String jobId, boolean populateTasks)
      throws IOException {
    Flow flow = null;
    JobKey key = idService.getJobKeyById(new QualifiedJobId(cluster, jobId));
    if (key != null) {
      byte[] startRow = ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes(key.getCluster()), Bytes.toBytes(key.getUserName()),
          Bytes.toBytes(key.getAppId()),
          Bytes.toBytes(key.getEncodedRunId()), Constants.EMPTY_BYTES);

      LOG.info("Reading job_history rows start at "
          + Bytes.toStringBinary(startRow));
      Scan scan = new Scan();
      // start scanning history at cluster!user!app!run!
      scan.setStartRow(startRow);
      // require that all results match this flow prefix
      scan.setFilter(new WhileMatchFilter(new PrefixFilter(startRow)));

      List<Flow> flows = createFromResults(scan, populateTasks, 1);
      if (flows.size() > 0) {
        flow = flows.get(0);
      }
    }
    return flow;
  }

  /**
   * creates a scan for flow data
   * @param rowPrefix - start row prefix
   * @param limit - limit on scanned results
   * @param version - version to match
   * @return Scan
   */
  private Scan createFlowScan(byte[] rowPrefix, int limit, String version) {
    Scan scan = new Scan();
    scan.setStartRow(rowPrefix);

    // using a large scanner caching value with a small limit can mean we scan a lot more data than
    // necessary, so lower the caching for low limits
    scan.setCaching(Math.min(limit, defaultScannerCaching));
    // require that all rows match the prefix we're looking for
    Filter prefixFilter = new WhileMatchFilter(new PrefixFilter(rowPrefix));
    // if version is passed, restrict the rows returned to that version
    if (version != null && version.length() > 0) {
      FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filters.addFilter(prefixFilter);
      filters.addFilter(new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
          Constants.VERSION_COLUMN_BYTES, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(version)));
      scan.setFilter(filters);
    } else {
      scan.setFilter(prefixFilter);
    }
    return scan;
  }

  /**
   * Returns the most recent {@link Flow} runs, up to {@code limit} instances.
   * If the {@code version} parameter is non-null, the returned results will be
   * restricted to those matching this app version.
   *
   * @param cluster
   *          the cluster where the jobs were run
   * @param user
   *          the user running the jobs
   * @param appId
   *          the application identifier for the jobs
   * @param version
   *          if non-null, only flows matching this application version will be
   *          returned
   * @param populateTasks
   *          if {@code true}, then TaskDetails will be populated for each job
   * @param limit
   *          the maximum number of flows to return
   * @return
   */
  public List<Flow> getFlowSeries(String cluster, String user, String appId,
      String version, boolean populateTasks, int limit) throws IOException {
    // TODO: use RunMatchFilter to limit scan on the server side
    byte[] rowPrefix = Bytes.toBytes(cluster + Constants.SEP + user
        + Constants.SEP + appId + Constants.SEP);
    Scan scan = createFlowScan(rowPrefix, limit, version);
    return createFromResults(scan, populateTasks, limit);
  }

  /**
   * Returns the most recent {@link Flow} runs within that time range,
   * up to {@code limit} instances.
   * If the {@code version} parameter is non-null, the returned results will be
   * restricted to those matching this app version.
   *
   * @param cluster
   *          the cluster where the jobs were run
   * @param user
   *          the user running the jobs
   * @param appId
   *          the application identifier for the jobs
   * @param version
   *          if non-null, only flows matching this application version will be
   *          returned
   * @param startTime
   *          the start time for the flows to be looked at
   * @param endTime
   *          the end time for the flows to be looked at
   * @param populateTasks
   *          if {@code true}, then TaskDetails will be populated for each job
   * @param limit
   *          the maximum number of flows to return
   * @return
   */
  public List<Flow> getFlowSeries(String cluster, String user, String appId, String version,
      boolean populateTasks, long startTime, long endTime, int limit) throws IOException {
    // TODO: use RunMatchFilter to limit scan on the server side
    byte[] rowPrefix =
        Bytes.toBytes(cluster + Constants.SEP + user + Constants.SEP + appId + Constants.SEP);
    Scan scan = createFlowScan(rowPrefix, limit, version);

    // set the start and stop rows for scan so that it's time bound
    if (endTime != 0) {
      byte[] scanStartRow;
      // use end time in start row, if present
      long endRunId = FlowKey.encodeRunId(endTime);
      scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(endRunId), Constants.SEP_BYTES);
      scan.setStartRow(scanStartRow);
    }

    if (startTime != 0) {
      byte[] scanStopRow;
      // use start time in stop row, if present
      long stopRunId = FlowKey.encodeRunId(startTime);
      scanStopRow = Bytes.add(rowPrefix, Bytes.toBytes(stopRunId), Constants.SEP_BYTES);
      scan.setStopRow(scanStopRow);
    }
    return createFromResults(scan, populateTasks, limit);
  }

  /**
   * Returns the {@link Flow} runs' stats - summed up per flow
   * If the {@code version} parameter is non-null, the returned results will be
   * restricted to those matching this app version.
   *
   * <p>
   *   <strong>Note:</strong> this retrieval method will omit the configuration data from
   *   all of the returned jobs.
   * </p>
   *
   * @param cluster
   *          the cluster where the jobs were run
   * @param user
   *          the user running the jobs
   * @param appId
   *          the application identifier for the jobs
   * @param version
   *          if non-null, only flows matching this application version will be
   *          returned
   * @param startTime
   *          the start time for the flows to be looked at
   * @param endTime
   *          the end time for the flows to be looked at
   * @param limit
   *          the maximum number of flows to return
   * @return
   */
  public List<Flow> getFlowTimeSeriesStats(String cluster, String user, String appId,
      String version, long startTime, long endTime, int limit, byte[] startRow) throws IOException {

    // app portion of row key
    byte[] rowPrefix = Bytes.toBytes((cluster + Constants.SEP + user + Constants.SEP
        + appId + Constants.SEP ));
    byte[] scanStartRow;

    if (startRow != null ) {
      scanStartRow = startRow;
    } else {
      if (endTime != 0) {
        // use end time in start row, if present
        long endRunId = FlowKey.encodeRunId(endTime);
        scanStartRow = Bytes.add(rowPrefix, Bytes.toBytes(endRunId), Constants.SEP_BYTES);
      } else {
        scanStartRow = rowPrefix;
      }
    }

    // TODO: use RunMatchFilter to limit scan on the server side
    Scan scan = new Scan();
    scan.setStartRow(scanStartRow);
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (startTime != 0) {
      // if limited by start time, early out as soon as we hit it
      long startRunId = FlowKey.encodeRunId(startTime);
      // zero byte at the end makes the startRunId inclusive
      byte[] scanEndRow = Bytes.add(rowPrefix, Bytes.toBytes(startRunId),
          Constants.ZERO_SINGLE_BYTE);
      scan.setStopRow(scanEndRow);
    } else {
      // require that all rows match the app prefix we're looking for
      filters.addFilter( new WhileMatchFilter(new PrefixFilter(rowPrefix)) );
    }

    // if version is passed, restrict the rows returned to that version
    if (version != null && version.length() > 0) {
      filters.addFilter(new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
          Constants.VERSION_COLUMN_BYTES, CompareFilter.CompareOp.EQUAL, Bytes
              .toBytes(version)));
    }

    // filter out all config columns except the queue name
    filters.addFilter(new QualifierFilter(
      CompareFilter.CompareOp.NOT_EQUAL,
        new RegexStringComparator(
          "^c\\!((?!" + Constants.HRAVEN_QUEUE + ").)*$")));

    scan.setFilter(filters);

    LOG.info("scan : \n " + scan.toJSON() + " \n");
    return createFromResults(scan, false, limit);
  }

  /**
   * Returns a specific job's data by job ID.  This version does not populate
   * the job's task data.
   * @param cluster the cluster identifier
   * @param cluster the job ID
   */
  public JobDetails getJobByJobID(String cluster, String jobId) throws IOException {
    return getJobByJobID(cluster, jobId, false);
  }

  /**
   * Returns a specific job's data by job ID
   * @param cluster the cluster identifier
   * @param cluster the job ID
   * @param populateTasks if {@code true} populate the {@link TaskDetails} records for the job
   */
  public JobDetails getJobByJobID(String cluster, String jobId, boolean populateTasks)
      throws IOException {
    return getJobByJobID(new QualifiedJobId(cluster, jobId), populateTasks);
  }

  /**
    * Returns a specific job's data by job ID
    * @param jobId the fully qualified cluster + job identifier
    * @param populateTasks if {@code true} populate the {@link TaskDetails} records for the job
    */
  public JobDetails getJobByJobID(QualifiedJobId jobId, boolean populateTasks)
      throws IOException {
    JobDetails job = null;
    JobKey key = idService.getJobKeyById(jobId);
    if (key != null) {
      byte[] historyKey = jobKeyConv.toBytes(key);
      Result result = historyTable.get(new Get(historyKey));
      if (result != null && !result.isEmpty()) {
        job = new JobDetails(key);
        job.populate(result);
        if (populateTasks) {
          populateTasks(job);
        }
      }
    }
    return job;
  }

  /**
   * Returns a list of {@link Flow} instances generated from the given results.
   * For the moment, this assumes that the given scanner provides results
   * ordered first by flow ID.
   * 
   * @param scan
   *          the Scan instance setup for retrieval
   * @return
   */
  private List<Flow> createFromResults(Scan scan, boolean populateTasks,
      int maxCount) throws IOException {
    List<Flow> flows = new ArrayList<Flow>();
    ResultScanner scanner = null;
    try {
      Stopwatch timer = new Stopwatch().start();
      Stopwatch timerJob = new Stopwatch();
      int rowCount = 0;
      long colCount = 0;
      long resultSize = 0;
      int jobCount = 0;
      scanner = historyTable.getScanner(scan);
      Flow currentFlow = null;
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          resultSize += result.getWritableSize();
          JobKey currentKey = jobKeyConv.fromBytes(result.getRow());
          // empty runId is special cased -- we need to treat each job as it's own flow
          if (currentFlow == null || !currentFlow.contains(currentKey) ||
              currentKey.getRunId() == 0) {
            // return if we've already hit the limit
            if (flows.size() >= maxCount) {
              break;
            }
            currentFlow = new Flow(new FlowKey(currentKey));
            flows.add(currentFlow);
          }
          timerJob.start();
          JobDetails job = new JobDetails(currentKey);
          job.populate(result);
          currentFlow.addJob(job);
          jobCount++;
          timerJob.stop();
        }
      }
      timer.stop();
      LOG.info("Fetched from hbase " + rowCount + " rows, " + colCount + " columns, "
          + flows.size() + " flows and " + jobCount + " jobs taking up "
          + resultSize + " bytes ( " + (double) resultSize / (1024.0 * 1024.0)
          + " atomic double: " + new AtomicDouble(resultSize / (1024.0 * 1024.0))
          + ") MB, in total time of " + timer + " with  " + timerJob
          + " spent inJobDetails & Flow population");

      // export the size of data fetched from hbase as a metric
      HravenResponseMetrics.FLOW_HBASE_RESULT_SIZE_VALUE.set((double) (resultSize / (1024.0 * 1024.0)));
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    if (populateTasks) {
      populateTasks(flows);
    }

    return flows;
  }

  /**
   * Populate the task details for the jobs in the given flows. <strong>Note
   * that all flows are expected to share the same cluster, user, and
   * appId.</strong>
   * 
   * @param flows
   */
  private void populateTasks(List<Flow> flows) throws IOException {
    if (flows == null || flows.size() == 0) {
      return;
    }

    // for simplicity, we assume that flows are ordered and consecutive
    JobKey startJob = null;
    // find the first job
    for (Flow f : flows) {
      List<JobDetails> jobs = f.getJobs();
      if (jobs != null && jobs.size() > 0) {
        startJob = jobs.get(0).getJobKey();
        break;
      }
    }

    if (startJob == null) {
      LOG.info("No start job found for flows");
      return;
    }

    byte[] startKey = Bytes.add(jobKeyConv.toBytes(startJob), Constants.SEP_BYTES);
    Scan scan = new Scan();
    scan.setStartRow(startKey);
    // expect a lot of tasks on average
    scan.setCaching(500);

    ResultScanner scanner = this.taskTable.getScanner(scan);
    try {
      Result currentResult = scanner.next();
      for (Flow f : flows) {
        for (JobDetails j : f.getJobs()) {
          // within each job we advance through the scanner til we pass keys
          // matching the current job
          while (currentResult != null && !currentResult.isEmpty()) {
            TaskKey taskKey = taskKeyConv.fromBytes(currentResult.getRow());
            // see if this task belongs to the current job
            int comparison = j.getJobKey().compareTo(taskKey);
            if (comparison < 0) {
              // advance to next job (without advancing current result)
              break;
            } else if (comparison > 0) {
              // advance tasks up to current job
            } else {
              // belongs to the current job
              TaskDetails task = new TaskDetails(taskKey);
              task.populate(currentResult
                  .getFamilyMap(Constants.INFO_FAM_BYTES));
              j.addTask(task);
            }
            currentResult = scanner.next();
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Added " + j.getTasks().size() + " tasks to job "
                + j.getJobKey().toString());
          }
        }
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * Populate the task details for a specific job.  To populate tasks for multiple
   * jobs together, use {@link JobHistoryService#populateTasks(java.util.List)}.
   * @param job
   */
  private void populateTasks(JobDetails job) throws IOException {
    // TODO: see if we can merge common logic here with populateTasks(List<Flow>)
    Scan scan = getTaskScan(job.getJobKey());
    ResultScanner scanner = this.taskTable.getScanner(scan);
    try {
      // advance through the scanner til we pass keys matching the job
      for (Result currentResult : scanner) {
        if (currentResult == null || currentResult.isEmpty()) {
          break;
        }

        TaskKey taskKey = taskKeyConv.fromBytes(currentResult.getRow());
        TaskDetails task = new TaskDetails(taskKey);
        task.populate(currentResult
            .getFamilyMap(Constants.INFO_FAM_BYTES));
        job.addTask(task);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added " + job.getTasks().size() + " tasks to job "
            + job.getJobKey().toString());
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * Returns a Scan instance to retrieve all the task rows for a given job
   * from the job_history_task table.
   * @param jobKey the job key to match for all task rows
   * @return a {@code Scan} instance for the job_history_task table
   */
  private Scan getTaskScan(JobKey jobKey) {
    byte[] startKey = Bytes.add(jobKeyConv.toBytes(jobKey), Constants.SEP_BYTES);
    Scan scan = new Scan();
    scan.setStartRow(startKey);
    // only return tasks for this job
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(startKey)));
    // expect a lot of tasks on average
    scan.setCaching(500);
    return scan;
  }

  /**
   * Converts serialized configuration properties back in to a Configuration
   * object.
   * 
   * @param keyValues
   * @return
   */
  public static Configuration parseConfiguration(Map<byte[], byte[]> keyValues) {
    Configuration config = new Configuration(false);
    byte[] configPrefix = Bytes.add(Constants.JOB_CONF_COLUMN_PREFIX_BYTES,
        Constants.SEP_BYTES);
    for (Map.Entry<byte[], byte[]> entry : keyValues.entrySet()) {
      byte[] key = entry.getKey();
      if (Bytes.startsWith(key, configPrefix)
          && key.length > configPrefix.length) {
        byte[] name = Bytes.tail(key, key.length - configPrefix.length);
        config.set(Bytes.toString(name), Bytes.toString(entry.getValue()));
      }
    }

    return config;
  }

  /**
   * Converts encoded key values back into counter objects.
   * 
   * @param keyValues
   * @return
   */
  public static CounterMap parseCounters(byte[] prefix,
      Map<byte[], byte[]> keyValues) {
    CounterMap counterValues = new CounterMap();
    byte[] counterPrefix = Bytes.add(prefix, Constants.SEP_BYTES);
    for (Map.Entry<byte[], byte[]> entry : keyValues.entrySet()) {
      byte[] key = entry.getKey();
      if (Bytes.startsWith(key, counterPrefix)
          && key.length > counterPrefix.length) {
        // qualifier should be in the format: g!countergroup!counterkey
        byte[][] qualifierFields = ByteUtil.split(
            Bytes.tail(key, key.length - counterPrefix.length),
            Constants.SEP_BYTES);
        if (qualifierFields.length != 2) {
          throw new IllegalArgumentException(
              "Malformed column qualifier for counter value: "
                  + Bytes.toStringBinary(key));
        }
        Counter c = new Counter(Bytes.toString(qualifierFields[0]),
            Bytes.toString(qualifierFields[1]), Bytes.toLong(entry.getValue()));
        counterValues.add(c);
      }
    }

    return counterValues;
  }

  /**
   * sets the hRavenQueueName in the jobPut
   * so that it's independent of hadoop1/hadoop2 queue/pool names
   *
   * @param jobConf
   * @param jobPut
   * @param jobKey
   * @param jobConfColumnPrefix
   *
   * @throws IllegalArgumentException if neither config param is found
   */
   static void setHravenQueueNameRecord(Configuration jobConf, JobHistoryRecordCollection recordCollection, JobKey jobKey) {

     String hRavenQueueName = HadoopConfUtil.getQueueName(jobConf);
     if (hRavenQueueName.equalsIgnoreCase(Constants.DEFAULT_VALUE_QUEUENAME)){
       // due to a bug in hadoop2, the queue name value is the string "default"
       // hence set it to username
       hRavenQueueName = jobKey.getUserName();
     }

     // set the "queue" property defined by hRaven
     // this makes it independent of hadoop version config parameters
	 recordCollection.add(RecordCategory.CONF_META, new RecordDataKey(
				Constants.HRAVEN_QUEUE), hRavenQueueName);
   }

  /**
   * Returns the HBase {@code Put} instances to store for the given
   * {@code Configuration} data. Each configuration property will be stored as a
   * separate key value.
   *
   * @param jobDesc
   *          the {@link JobDesc} generated for the job
   * @param jobConf
   *          the job configuration
   * @return puts for the given job configuration
   */
  public static JobHistoryRecordCollection getConfRecord(JobDesc jobDesc, Configuration jobConf) {
    JobKey jobKey = new JobKey(jobDesc);

    // Add all columns to one put
    JobHistoryRecordCollection recordCollection = new JobHistoryRecordCollection(jobKey);

    recordCollection.add(RecordCategory.CONF_META, new RecordDataKey(Constants.VERSION_COLUMN),
      jobDesc.getVersion());
    recordCollection.add(RecordCategory.CONF_META, new RecordDataKey(Constants.FRAMEWORK_COLUMN), jobDesc
        .getFramework().toString());

    // Create records for all the parameters in the job configuration
    Iterator<Entry<String, String>> jobConfIterator = jobConf.iterator();
    while (jobConfIterator.hasNext()) {
      Entry<String, String> entry = jobConfIterator.next();
      recordCollection.add(RecordCategory.CONF, new RecordDataKey(entry.getKey()), entry.getValue());
    }

    // ensure pool/queuename is set correctly
    setHravenQueueNameRecord(jobConf, recordCollection, jobKey);

    return recordCollection;
  }

  /**
   * Removes the job's row from the job_history table, and all related task rows
   * from the job_history_task table.
   * @param key the job to be removed
   * @return the number of rows deleted.
   * @throws IOException
   */
  public int removeJob(JobKey key) throws IOException {
    byte[] jobRow = jobKeyConv.toBytes(key);
     
    historyTable.delete(new Delete(jobRow));
    
    int deleteCount = 1;

    // delete all task rows
    Scan taskScan = getTaskScan(key);
    // only need the row keys back to delete (all should have taskid)
    taskScan.addColumn(Constants.INFO_FAM_BYTES,
        JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TASKID));
    // no reason to cache rows we're deleting
    taskScan.setCacheBlocks(false);
    List<Delete> taskDeletes = new ArrayList<Delete>();
    ResultScanner scanner = taskTable.getScanner(taskScan);
    try {
      for (Result r : scanner) {
        if (r != null && !r.isEmpty()) {
          byte[] rowKey = r.getRow();
          TaskKey taskKey = taskKeyConv.fromBytes(rowKey);
          if (!key.equals(taskKey)) {
            LOG.warn("Found task not in the current job "+Bytes.toStringBinary(rowKey));
            break;
          }
          taskDeletes.add(new Delete(r.getRow()));
        }
      }
      // Hang on the count because delete will modify our list.
      deleteCount += taskDeletes.size();
      if (taskDeletes.size() > 0) {
        LOG.info("Deleting "+taskDeletes.size()+" tasks for job "+key);
        taskTable.delete(taskDeletes);
      }
    } finally {
      scanner.close();
    }
    return deleteCount;
  }

  /**
   * Cleans up the internal HBase table instances. This should always be called
   * when the service instance is being released.
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    IOException caught = null;
    if (this.historyTable != null) {
      try {
        this.historyTable.close();
      } catch (IOException ioe) {
        caught = ioe;
      }
    }
    if (this.idService != null) {
      try {
        this.idService.close();
      } catch (IOException ioe) {
        // TODO: don't overwrite a previous exception
        caught = ioe;
      }
    }
    if (this.taskTable != null) {
      try {
        this.taskTable.close();
      } catch (IOException ioe) {
        // TODO: don't overwrite a previous exception
        caught = ioe;
      }
    }
    if (caught != null) {
      throw caught;
    }
  }

}
