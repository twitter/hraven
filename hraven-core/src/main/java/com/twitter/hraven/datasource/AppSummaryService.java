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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
  private final HTable versionsTable;
  private final HTable aggDailyTable;
  private final HTable aggWeeklyTable;

  private AppAggregationKeyConverter aggConv = new AppAggregationKeyConverter();

  public AppSummaryService(Configuration hbaseConf) throws IOException {
    this.conf = hbaseConf;
    this.versionsTable = new HTable(conf, Constants.HISTORY_APP_VERSION_TABLE);
    this.aggDailyTable = new HTable(conf, AggregationConstants.AGG_DAILY_TABLE);
    this.aggWeeklyTable = new HTable(conf, AggregationConstants.AGG_WEEKLY_TABLE);
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
  private AppKey getNewAppKeyFromResult(Result result, Long startTime, Long endTime)
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

  /**
   * inserts aggregations for job details for that week
   * @param {@link JobDetails}
   */
  public void aggregateJobDetailsWeekly(JobDetails jobDetails) {
    // create row key
    JobKey jobKey = jobDetails.getJobKey();
    AppAggregationKey appAggKey = new AppAggregationKey(
             jobKey.getCluster(),
             jobKey.getUserName(),
             jobKey.getAppId(),
            getWeekTimestamp(jobKey.getRunId()));
    LOG.trace("in aggregateJobDetailsWeekly " + jobKey.toString());
    Increment aggWeeklyIncrement = incrementAppSummary(appAggKey, jobDetails);
    try {
      aggWeeklyTable.increment(aggWeeklyIncrement);
      Put p = getMoreAggInfo(appAggKey, jobDetails);
      aggWeeklyTable.put(p);
    } catch (IOException e) {
      LOG.error("Error while incrementing daily aggregations " + e);
    }
  }

  /**
   * creates a list of puts that aggregate the job details and stores in daily aggregation table
   * @param {@link JobDetails}
   */
  public void aggregateJobDetailsDaily(JobDetails jobDetails) {
    // create row key
    JobKey jobKey = jobDetails.getJobKey();
    AppAggregationKey appAggKey = new AppAggregationKey(
            jobKey.getCluster(),
            jobKey.getUserName(),
            jobKey.getAppId(),
            getDayTimestamp(jobKey.getRunId()));
    LOG.trace(" in aggregateJobDetailsDaily  " + jobKey.toString());
    Increment aggDailyIncrement = incrementAppSummary(appAggKey, jobDetails);
    try {
      aggDailyTable.increment(aggDailyIncrement);
      Put p = getMoreAggInfo(appAggKey, jobDetails);
      aggDailyTable.put(p);
    } catch (IOException e) {
      LOG.error("Error while incrementing daily aggregations " + e);
    }
  }

  /**
   * interprets the number of runs based on number of columns in raw col family
   * @param {@link Result}
   * @return number of runs
   */
  long getNumberRuns(Map<byte[], byte[]> rawFamily) {
    long numberRuns = 0L;
    if (rawFamily != null) {
      numberRuns = rawFamily.size();
    }
    return numberRuns;
  }

  /**
   * looks at {@Link Result} to see if queue name already is stored,
   * if not will store it
   * @param {@link JobDetails}
   * @param {@link Result}
   * @return queue list
   */
  String createQueueListValue(JobDetails jobDetails, KeyValue existingQueuesKV) {
    /*
     * get the existing list of queues for this app summary, if this job's queue not already
     * included, append it to existing set
     */
    String existingQueues = "";
    if (existingQueuesKV != null) {
      existingQueues = Bytes.toString(existingQueuesKV.getValue());
    }
    // check if queue already exists
    // append separator at the end to avoid "false" queue match via substring match
    String queue = jobDetails.getQueue();
    queue = queue.concat(Constants.SEP);
    if (!existingQueues.contains(queue)) {
      existingQueues = existingQueues.concat(queue);
    }
    LOG.trace(" existingQueues " + existingQueues);
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

    LOG.trace(" incr " + aggIncrement.numColumns() + " " + aggIncrement.toString());
    return aggIncrement;
  }

  /**
   * find out the top of the day timestamp
   * @param runId
   * @return top of the day timestamp
   */
  Long getDayTimestamp(long runId) {
    // get top of the hour
    long dayTimestamp = runId - (runId % Constants.MILLIS_ONE_DAY);
    return dayTimestamp;
  }

  /**
   * find out the top of the week timestamp
   * @param runId
   * @return top of the day timestamp
   */
  Long getWeekTimestamp(long runId) {
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

  /**
   * creates {@link Put} for inserting additional info into agg tables
   * @param {@link AppAggregationKey}
   * @param {@link JobDetails}
   * @return {@link Put}
   * @throws IOException
   */
  private Put getMoreAggInfo(AppAggregationKey appAggKey, JobDetails jobDetails)
      throws IOException {

    byte[] rowKey = aggConv.toBytes(appAggKey);
    // get the existing number of runs and list of queues
    // for this app aggregation
    Get g = new Get(rowKey);
    g.addColumn(AggregationConstants.INFO_FAM_BYTES, AggregationConstants.HRAVEN_QUEUE_BYTES);
    g.addColumn(AggregationConstants.INFO_FAM_BYTES, AggregationConstants.JOBCOST_BYTES);
    g.addFamily(AggregationConstants.SCRATCH_FAM_BYTES);
    Result r = aggDailyTable.get(g);

    long numberRuns = getNumberRuns(r.getFamilyMap(AggregationConstants.SCRATCH_FAM_BYTES));
    String queue = createQueueListValue(jobDetails,
        r.getColumnLatest(AggregationConstants.INFO_FAM_BYTES,
          AggregationConstants.HRAVEN_QUEUE_BYTES));
    Double cost = getCost(jobDetails,
        r.getColumnLatest(AggregationConstants.INFO_FAM_BYTES,
          AggregationConstants.JOBCOST_BYTES));

    // create a put for username, appid, queue set and number of runs
    Put p = new Put(rowKey);
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.USER_BYTES,
      Bytes.toBytes(appAggKey.getUserName()));
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.APP_ID_COL_BYTES,
      Bytes.toBytes(appAggKey.getAppId()));
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.NUMBER_RUNS_BYTES,
      Bytes.toBytes(numberRuns));
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.HRAVEN_QUEUE_BYTES,
      Bytes.toBytes(queue));
    p.add(Constants.INFO_FAM_BYTES, AggregationConstants.JOBCOST_BYTES,
      Bytes.toBytes(cost));
    return p;
  }

  /**
   * adds up the cost of an app
   * @param jobDetails
   * @param {@link KeyValue}
   * @return {@link Double} cost
   */
  private Double getCost(JobDetails jobDetails, KeyValue columnLatest) {
    Double cost = 0.0;
    if (columnLatest != null) {
      cost = Bytes.toDouble(columnLatest.getValue());
    }
    return (jobDetails.getCost() + cost);
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
    long topDayEndTime = Long.MAX_VALUE - getDayTimestamp(endTime) - 1;
    // set the time to top of the day plus 1 to make sure that timestamp is included
    long topDayStartTime = Long.MAX_VALUE - getDayTimestamp(startTime) + 1;

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
          resultSize += result.getWritableSize();
          apptimer.start();
          byte[] rowKey = result.getRow();
          AppAggregationKey appAggKey = aggConv.fromBytes(rowKey);
          AppKey ak = new AppKey(cluster, appAggKey.getUserName(), appAggKey.getAppId());
          AppSummary as1 = null;
          if (amap.containsKey(ak)) {
            as1 = amap.get(ak);
          } else {
            as1 = new AppSummary(ak);
            as1.setFirstRunId(appAggKey.getRunId());
            as1.setLastRunId(appAggKey.getRunId());
          }
          if (appAggKey.getRunId() < as1.getFirstRunId()) {
            as1.setFirstRunId(appAggKey.getRunId());
          }
          if (appAggKey.getRunId() > as1.getLastRunId()) {
            as1.setLastRunId(appAggKey.getRunId());
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
          + timer.elapsedMillis() + " that includes \n appSummary population timer of "
          + apptimer + " elapsedMillis" + apptimer.elapsedMillis() + " \n hbase scan time is "
          + (timer.elapsedMillis() - apptimer.elapsedMillis()));
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
