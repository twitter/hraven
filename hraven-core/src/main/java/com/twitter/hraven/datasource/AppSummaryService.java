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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Stopwatch;
import com.twitter.hraven.AggregationConstants;
import com.twitter.hraven.AppAggregationKey;
import com.twitter.hraven.AppSummary;
import com.twitter.hraven.AppKey;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.util.ByteUtil;

/**
 * Reads and writes information about applications
 */
public class AppSummaryService {

  private static final Log LOG = LogFactory.getLog(AppSummaryService.class);
  private final Configuration conf;
  private final Table versionsTable;
  private final Table aggDailyTable;
  private final Table aggWeeklyTable;

  private AppAggregationKeyConverter aggConv = new AppAggregationKeyConverter();

  public AppSummaryService(Configuration hbaseConf) throws IOException {
    if (hbaseConf == null) {
      conf = new Configuration();
    } else {
      conf = hbaseConf;
    }

    Connection conn = ConnectionFactory.createConnection(conf);

    this.versionsTable = conn.getTable(TableName.valueOf(Constants.HISTORY_APP_VERSION_TABLE));
    this.aggDailyTable = conn.getTable(TableName.valueOf(AggregationConstants.AGG_DAILY_TABLE));
    this.aggWeeklyTable = conn.getTable(TableName.valueOf(AggregationConstants.AGG_WEEKLY_TABLE));
  }

  /**
   * scans the app version table to look for jobs that showed up in the given time range
   * creates the flow key that maps to these apps
   * @param cluster
   * @param user
   * @param startTime
   * @param endTime
   * @param limit
   * @return list of flow keys
   * @throws IOException
   * @throws ProcessingException
   */
  public List<AppSummary> getNewApps(JobHistoryService jhs, String cluster,
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

    List<AppSummary> newApps = new ArrayList<AppSummary>();
    for (AppKey ak : newAppsKeys) {
      AppSummary anApp = new AppSummary(ak);
      List<Flow> flows =
          jhs.getFlowSeries(ak.getCluster(), ak.getUserName(), ak.getAppId(), null, Boolean.FALSE,
            startTime, endTime, Integer.MAX_VALUE);
      for (Flow f : flows) {
        anApp.addFlow(f);
      }
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
  public List<AppKey> createNewAppKeysFromResults(Scan scan, long startTime, long endTime, int maxCount)
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
     //TODO dogpiledays     resultSize += result.getWritableSize();
          AppKey appKey = getNewAppKeyFromResult(result, startTime, endTime);
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
   * constructs App key from the result set based on cluster, user, appId
   * picks those results that satisfy the time range criteria
   * @param result
   * @param startTime
   * @param endTime
   * @return flow key
   * @throws IOException
   */
  private AppKey getNewAppKeyFromResult(Result result, long startTime, long endTime)
      throws IOException {

    byte[] rowKey = result.getRow();
    byte[][] keyComponents = ByteUtil.split(rowKey, Constants.SEP_BYTES);
    String cluster = Bytes.toString(keyComponents[0]);
    String user = Bytes.toString(keyComponents[1]);
    String appId = Bytes.toString(keyComponents[2]);

    NavigableMap<byte[],byte[]> valueMap = result.getFamilyMap(Constants.INFO_FAM_BYTES);
    long runId = Long.MAX_VALUE;
    for (Map.Entry<byte[],byte[]> entry : valueMap.entrySet()) {
      long tsl = Bytes.toLong(entry.getValue()) ;
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

  /**
   * creates a list of puts that aggregate the job details and stores
   * in daily or weekly aggregation table
   * @param {@link JobDetails}
   */
  public boolean aggregateJobDetails(JobDetails jobDetails,
      AggregationConstants.AGGREGATION_TYPE aggType) {
    Table aggTable = aggDailyTable;
    switch (aggType) {
    case DAILY:
      aggTable = aggDailyTable;
      break;
    case WEEKLY:
      aggTable = aggWeeklyTable;
      break;
    default:
      LOG.error("Unknown aggregation type : " + aggType);
        return false;
    }
    try {
      // create row key
      JobKey jobKey = jobDetails.getJobKey();
      AppAggregationKey appAggKey =
          new AppAggregationKey(jobKey.getCluster(), jobKey.getUserName(), jobKey.getAppId(),
              getTimestamp(jobKey.getRunId(), aggType));
      LOG.info("Aggregating " + aggType + " stats for  " + jobKey.toString());
      Increment aggIncrement = incrementAppSummary(appAggKey, jobDetails);
      aggTable.increment(aggIncrement);
      boolean status = updateMoreAggInfo(aggTable, appAggKey, jobDetails);
      return status;
    }
    /*
     * try to catch all exceptions so that processing
     * is unaffected by aggregation errors this can
     * be turned off in the future when we determine
     * aggregation to be a mandatory part of Processing Step
     */
    catch (Exception e) {
      LOG.error("Caught exception while attempting to aggregate for "
          + aggType + " table ", e);
      return false;
    }
  }

  /**
   * interprets the number of runs based on number of columns in raw col family
   * @param {@link Result}
   * @return number of runs
   */
  long getNumberRunsScratch(Map<byte[], byte[]> rawFamily) {
    long numberRuns = 0L;
    if (rawFamily != null) {
      numberRuns = rawFamily.size();
    }
    if(numberRuns == 0L) {
      LOG.error("Number of runs in scratch column family can't be 0,"
        +" if processing within TTL");
      throw new ProcessingException("Number of runs is 0");
    }
    return numberRuns;
  }

  /**
   * looks at {@Link String} to see if queue name already is stored,
   * if not, adds it
   * @param {@link JobDetails}
   * @param {@link Result}
   * @return queue list
   */
  String createQueueListValue(JobDetails jobDetails, String existingQueues) {
    /*
     * check if queue already exists
     * append separator at the end to avoid "false" queue match via substring match
     */
    String queue = jobDetails.getQueue();
    queue = queue.concat(Constants.SEP);

    if (existingQueues == null ) {
      return queue;
    }

    if (!existingQueues.contains(queue)) {
      existingQueues = existingQueues.concat(queue);
    }
    return existingQueues;
  }

  /**
   * creates an Increment to aggregate job details
   * @param {@link AppAggregationKey}
   * @param {@link JobDetails}
   * @return {@link Increment}
   */
  private Increment incrementAppSummary(AppAggregationKey appAggKey, JobDetails jobDetails) {
    Increment aggIncrement = new Increment(aggConv.toBytes(appAggKey));
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES, AggregationConstants.TOTAL_MAPS_BYTES,
      jobDetails.getTotalMaps());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES, AggregationConstants.TOTAL_REDUCES_BYTES,
      jobDetails.getTotalReduces());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES, AggregationConstants.MEGABYTEMILLIS_BYTES,
      jobDetails.getMegabyteMillis());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES, AggregationConstants.SLOTS_MILLIS_MAPS_BYTES,
      jobDetails.getMapSlotMillis());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES,
      AggregationConstants.SLOTS_MILLIS_REDUCES_BYTES, jobDetails.getReduceSlotMillis());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES,
      AggregationConstants.SLOTS_MILLIS_REDUCES_BYTES, jobDetails.getReduceSlotMillis());
    aggIncrement.addColumn(Constants.INFO_FAM_BYTES, AggregationConstants.TOTAL_JOBS_BYTES, 1L);

    byte[] numberRowsCol = Bytes.toBytes(jobDetails.getJobKey().getRunId());
    aggIncrement.addColumn(AggregationConstants.SCRATCH_FAM_BYTES, numberRowsCol, 1L);

    return aggIncrement;
  }

  /**
   * find out the top of the day/week timestamp
   * @param runId
   * @return top of the day/week timestamp
   */
  long getTimestamp(long runId, AggregationConstants.AGGREGATION_TYPE aggType) {
    if (AggregationConstants.AGGREGATION_TYPE.DAILY.equals(aggType)) {
      // get top of the hour
      long dayTimestamp = runId - (runId % Constants.MILLIS_ONE_DAY);
      return dayTimestamp;
    } else if (AggregationConstants.AGGREGATION_TYPE.WEEKLY.equals(aggType)) {
      // get top of the hour
      Calendar c = Calendar.getInstance();
      c.setTimeInMillis(runId);
      int d = c.get(Calendar.DAY_OF_WEEK);
      // get the first day of the week
      long weekTimestamp = runId - (d - 1) * Constants.MILLIS_ONE_DAY;
      // get the top of the day for that first day
      weekTimestamp = weekTimestamp - weekTimestamp % Constants.MILLIS_ONE_DAY;
      return weekTimestamp;
    }
    return 0L;
  }

  /**
   * creates {@link Put} for inserting additional info into agg tables
   * @param {@link AppAggregationKey}
   * @param {@link JobDetails}
   * @return {@link Put}
   * @throws IOException
   */
  boolean updateMoreAggInfo(Table aggTable, AppAggregationKey appAggKey,
        JobDetails jobDetails) throws IOException {

    int attempts = 0;
    boolean status = false;
    // update number of runs
    while ((!status) && (attempts < AggregationConstants.RETRY_COUNT)) {
      status = updateNumberRuns(appAggKey, aggTable, jobDetails);
      attempts++;
    }

    // update queue
    attempts = 0;
    status = false;
    while ((!status) && (attempts < AggregationConstants.RETRY_COUNT)) {
      status = updateQueue(appAggKey, aggTable, jobDetails);
      attempts++;
    }

    // update cost
    attempts = 0;
    status = false;
    while ((!status) && (attempts < AggregationConstants.RETRY_COUNT)) {
      status = updateCost(appAggKey, aggTable, jobDetails);
      attempts++;
    }

    // create a put for username, appid
    byte[] rowKey = aggConv.toBytes(appAggKey);
    Put p = new Put(rowKey);
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.USER_BYTES,
      Bytes.toBytes(appAggKey.getUserName()));
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.APP_ID_COL_BYTES,
      Bytes.toBytes(appAggKey.getAppId()));
    aggTable.put(p);
    return true;
  }

  private boolean updateCost(AppAggregationKey appAggKey, Table aggTable,
      JobDetails jobDetails) throws IOException {
    byte[] rowKey = aggConv.toBytes(appAggKey);

    Get g = new Get(rowKey);
    g.addColumn(AggregationConstants.INFO_FAM_BYTES,
        AggregationConstants.JOBCOST_BYTES);
    Result r = aggTable.get(g);
    double existingCost = 0.0;
    byte[] existingCostBytes = null;
    KeyValue columnLatest = r.getColumnLatest(AggregationConstants.INFO_FAM_BYTES,
          AggregationConstants.JOBCOST_BYTES);

    if (columnLatest != null) {
      existingCost = Bytes.toDouble(columnLatest.getValue());
      existingCostBytes = Bytes.toBytes(existingCost);
    }

    double newCost = existingCost + jobDetails.getCost();
    if (LOG.isTraceEnabled()) {
      LOG.trace(" total app aggregated cost  " + newCost);
    }
    // now insert cost
    return executeCheckAndPut(aggTable, rowKey, existingCostBytes,
      Bytes.toBytes(newCost),
      AggregationConstants.INFO_FAM_BYTES,
      AggregationConstants.JOBCOST_BYTES);

  }

  /**
   * updates the queue list for this app aggregation
   * @throws IOException
   */
  boolean updateQueue(AppAggregationKey appAggKey, Table aggTable, JobDetails jobDetails)
      throws IOException {
    byte[] rowKey = aggConv.toBytes(appAggKey);

    Get g = new Get(rowKey);
    g.addColumn(AggregationConstants.INFO_FAM_BYTES,
        AggregationConstants.HRAVEN_QUEUE_BYTES);
    Result r = aggTable.get(g);

    KeyValue existingQueuesKV =
        r.getColumnLatest(AggregationConstants.INFO_FAM_BYTES,
          AggregationConstants.HRAVEN_QUEUE_BYTES);
    String existingQueues = null;
    byte[] existingQueuesBytes = null;
    if (existingQueuesKV != null) {
      existingQueues = Bytes.toString(existingQueuesKV.getValue());
      existingQueuesBytes = Bytes.toBytes(existingQueues);
    }

    // get details for the queue list to be inserted
    String insertQueues = createQueueListValue(jobDetails, existingQueues);

    // if existing and to be inserted queue lists are different, then
    // execute check and put
    if (insertQueues.equalsIgnoreCase(existingQueues)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Queue already present in aggregation for this app "
            + existingQueues + " " + insertQueues);
      }
      return true;
    } else {
      return executeCheckAndPut(aggTable, rowKey, existingQueuesBytes,
        Bytes.toBytes(insertQueues),
        AggregationConstants.INFO_FAM_BYTES,
        AggregationConstants.HRAVEN_QUEUE_BYTES);
    }
  }

  /**
   * method to execute an hbase checkAndPut operation
   * @return whether or not the check and put was successful after retries
   * @throws IOException
   */
  boolean executeCheckAndPut(Table aggTable, byte[] rowKey, byte[] existingValueBytes,
      byte[] newValueBytes, byte[] famBytes, byte[] colBytes) throws IOException {

    Put put = new Put(rowKey);
    put.add(famBytes, colBytes, newValueBytes);

    boolean statusCheckAndPut = aggTable.checkAndPut(rowKey,
        famBytes,
        colBytes,
        existingValueBytes,
        put);
    return statusCheckAndPut;

  }

  byte[] getCurrentValue(Table aggTable, byte[] rowKey, byte[] famBytes,
      byte[] colBytes) throws IOException {
    Get g = new Get(rowKey);
    g.addColumn(famBytes, colBytes);
    Result r = aggTable.get(g);
   return r.getValue(famBytes, colBytes);
  }

  private boolean updateNumberRuns(AppAggregationKey appAggKey, Table aggTable,
      JobDetails jobDetails) throws IOException {

    byte[] rowKey = aggConv.toBytes(appAggKey);

    Get g = new Get(rowKey);
    g.addColumn(AggregationConstants.INFO_FAM_BYTES,
        AggregationConstants.NUMBER_RUNS_BYTES);
    byte[] numberRowsCol = Bytes.toBytes(jobDetails.getJobKey().getRunId());
    g.addColumn(AggregationConstants.SCRATCH_FAM_BYTES, numberRowsCol);
    Result r = aggTable.get(g);

    if (LOG.isTraceEnabled()) {
      LOG.trace(" jobkey " + jobDetails.getJobKey().toString()
          + " runid in updateNumberRuns " + jobDetails.getJobKey().getRunId());
    }
    long numberRuns = getNumberRunsScratch(r.getFamilyMap
        (AggregationConstants.SCRATCH_FAM_BYTES));
    if (numberRuns == 1L) {
      // generate check and put
      // since first run id was inserted, we increment the number of runs
      // it is possible that some other map task inserted
      // the first run id for a job in this flow
      // hence we check and put the number of runs in info col family
      return incrNumberRuns(r.getColumn
            (AggregationConstants.INFO_FAM_BYTES,
            AggregationConstants.NUMBER_RUNS_BYTES),
            aggTable, appAggKey);
    } else {
      // no need to update, since this was not
      // the first app seen for this run id
      return true;
    }
  }

  /**
   * checks and increments the number of runs for this app aggregation.
   * no need to retry, since another
   * map task may have updated it in the mean time
   * @throws IOException
   */
  boolean incrNumberRuns(List<KeyValue> column, Table aggTable, AppAggregationKey appAggKey)
      throws IOException {

    /*
     * check if this is the very first insert for numbers for this app in that case,
     * there will no existing column/value for number of run in info col family
     * null signifies non existence of the column for {@link Table.checkAndPut}
     */
    long expectedValueBeforePut = 0L;
    if (column.size() > 0) {
      try {
        expectedValueBeforePut = Bytes.toLong(column.get(0).getValue());
      } catch (NumberFormatException e) {
        LOG.error("Could not read existing value for number of runs during aggregation"
            + appAggKey.toString());
        return false;
      }
    }

    byte[] rowKey = aggConv.toBytes(appAggKey);

    long insertValue = 1L;
    byte[] expectedValueBeforePutBytes = null;
    if (expectedValueBeforePut != 0L) {
      insertValue = 1 + expectedValueBeforePut;
      expectedValueBeforePutBytes = Bytes.toBytes(expectedValueBeforePut);
    }
    byte[] insertValueBytes = Bytes.toBytes(insertValue);

    if (LOG.isTraceEnabled()) {
      LOG.trace(" before statusCheckAndPut " + insertValue + " "
          + expectedValueBeforePut);
    }
    return executeCheckAndPut(aggTable,
          rowKey,
          expectedValueBeforePutBytes,
          insertValueBytes,
          AggregationConstants.INFO_FAM_BYTES,
          AggregationConstants.NUMBER_RUNS_BYTES);
  }

  /**
   * gets list of all apps in the specified time frame from the aggregate tables
   * @param cluster
   * @param user
   * @param startTime
   * @param endTime
   * @param limit
   * @return {@link List < AppSummary >}
   * @throws IOException
   */
  public List<AppSummary> getAllApps(String cluster, String user, long startTime,
    long endTime, int limit) throws IOException {
    // set the time to top of the day minus 1 to make sure that timestamp is included
    long topDayEndTime = Long.MAX_VALUE - getTimestamp(endTime,
      AggregationConstants.AGGREGATION_TYPE.DAILY) - 1;
    // set the time to top of the day plus 1 to make sure that timestamp is included
    long topDayStartTime = Long.MAX_VALUE - getTimestamp(startTime,
      AggregationConstants.AGGREGATION_TYPE.DAILY) + 1;

    byte[] startRow = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(cluster),
        Bytes.toBytes(topDayEndTime));
    byte[] endRow = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(cluster),
        Bytes.toBytes(topDayStartTime));

    // start scanning agg table at cluster!inv timestamp![user]
    Scan scan = new Scan();

    if (StringUtils.isNotBlank(user)) {
      startRow = ByteUtil.join(Constants.SEP_BYTES, startRow, Bytes.toBytes(user));
      endRow = ByteUtil.join(Constants.SEP_BYTES, endRow, Bytes.toBytes(user));
      FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filters.addFilter(new SingleColumnValueFilter(Constants.INFO_FAM_BYTES,
          AggregationConstants.USER_BYTES, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(user)));
      scan.setFilter(filters);
    }
    scan.setStartRow(startRow);
    scan.setStopRow(endRow);
    LOG.info(" scan is " + scan.toJSON());

    Map<AppKey, AppSummary> amap = new HashMap<AppKey, AppSummary>();
    Stopwatch apptimer = new Stopwatch();

    ResultScanner scanner = null;
    try {
      Stopwatch timer = new Stopwatch().start();
      int rowCount = 0;
      long colCount = 0;
      long resultSize = 0;
      scanner = aggDailyTable.getScanner(scan);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
//TODO dogpile days          resultSize += result.getWritableSize();
          apptimer.start();
          byte[] rowKey = result.getRow();
          AppAggregationKey appAggKey = aggConv.fromBytes(rowKey);
          AppKey ak = new AppKey(cluster, appAggKey.getUserName(), appAggKey.getAppId());
          AppSummary as1 = null;
          if (amap.containsKey(ak)) {
            as1 = amap.get(ak);
          } else {
            as1 = new AppSummary(ak);
            as1.setFirstRunId(appAggKey.getAggregationId());
            as1.setLastRunId(appAggKey.getAggregationId());
          }
          if (appAggKey.getAggregationId() < as1.getFirstRunId()) {
            as1.setFirstRunId(appAggKey.getAggregationId());
          }
          if (appAggKey.getAggregationId() > as1.getLastRunId()) {
            as1.setLastRunId(appAggKey.getAggregationId());
          }
          amap.put(ak, populateAppSummary(result, as1));
          if (amap.size() >= limit) {
            break;
          }
          apptimer.stop();
        }
      }
      timer.stop();
      LOG.info(" Fetched from hbase " + rowCount + " rows, " + colCount + " columns, "
          + resultSize + " bytes ( " + resultSize / (1024 * 1024)
          + ") MB, in \n total timer of " + timer + " elapsedMillis:"
          + timer.elapsed(TimeUnit.MILLISECONDS) + " that includes \n appSummary population timer of "
          + apptimer + " elapsedMillis" + apptimer.elapsed(TimeUnit.MILLISECONDS)
          + " \n hbase scan time is "
          + (timer.elapsed(TimeUnit.MILLISECONDS) - apptimer.elapsed(TimeUnit.MILLISECONDS)));
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    LOG.info("Number of distinct apps " + amap.size());
    return new ArrayList<AppSummary>(amap.values());
  }

  private AppSummary populateAppSummary(Result result, AppSummary as) {

    NavigableMap<byte[], byte[]> infoValues = result.getFamilyMap(Constants.INFO_FAM_BYTES);
    as.setTotalMaps(as.getTotalMaps()
        + ByteUtil.getValueAsLong(AggregationConstants.TOTAL_MAPS_BYTES, infoValues));
    as.setTotalReduces(as.getTotalReduces()
        + ByteUtil.getValueAsLong(AggregationConstants.TOTAL_REDUCES_BYTES, infoValues));
    as.setMbMillis(as.getMbMillis()
        + ByteUtil.getValueAsLong(AggregationConstants.MEGABYTEMILLIS_BYTES, infoValues));
    as.setCost(as.getCost()
        + ByteUtil.getValueAsDouble(AggregationConstants.JOBCOST_BYTES, infoValues));
    as.setJobCount(as.getJobCount()
        + ByteUtil.getValueAsLong(AggregationConstants.TOTAL_JOBS_BYTES, infoValues));
    as.setNumberRuns(as.getNumberRuns()
        + ByteUtil.getValueAsLong(AggregationConstants.NUMBER_RUNS_BYTES, infoValues));
    as.setMapSlotMillis(as.getMapSlotMillis()
        + ByteUtil.getValueAsLong(AggregationConstants.SLOTS_MILLIS_MAPS_BYTES, infoValues));
    as.setReduceSlotMillis(as.getReduceSlotMillis()
        + ByteUtil.getValueAsLong(AggregationConstants.SLOTS_MILLIS_REDUCES_BYTES, infoValues));
    as.setQueuesFromString(ByteUtil.getValueAsString(AggregationConstants.HRAVEN_QUEUE_BYTES,
      infoValues));

    return as;
  }

}
