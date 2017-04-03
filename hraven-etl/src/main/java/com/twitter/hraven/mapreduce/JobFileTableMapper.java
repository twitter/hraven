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
package com.twitter.hraven.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import com.twitter.hraven.AggregationConstants;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.AppSummaryService;
import com.twitter.hraven.datasource.AppVersionService;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.MissingColumnInResultException;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.RowKeyParseException;
import com.twitter.hraven.etl.JobHistoryFileParser;
import com.twitter.hraven.etl.JobHistoryFileParserBase;
import com.twitter.hraven.etl.JobHistoryFileParserFactory;
import com.twitter.hraven.etl.ProcessRecordService;
import com.twitter.hraven.util.HadoopConfUtil;

/**
 * Takes in results from a scan from {@link ProcessRecordService
 * @getHistoryRawTableScan}, process both job file and history file and emit out
 *                           as puts for the {@link Constants#HISTORY_TABLE}
 *                           <p>
 *                           As a side-affect we'll load an index record into
 *                           the {@link Constants#HISTORY_BY_JOBID_TABLE} as
 *                           well.
 *
 */
public class JobFileTableMapper
    extends TableMapper<ImmutableBytesWritable, Put> {

  private static Log LOG = LogFactory.getLog(JobFileTableMapper.class);

  private static final ImmutableBytesWritable JOB_TABLE =
      new ImmutableBytesWritable(Bytes.toBytes(Constants.HISTORY_TABLE));
  private static final ImmutableBytesWritable TASK_TABLE =
      new ImmutableBytesWritable(Bytes.toBytes(Constants.HISTORY_TASK_TABLE));
  private static final ImmutableBytesWritable RAW_TABLE =
      new ImmutableBytesWritable(Bytes.toBytes(Constants.HISTORY_RAW_TABLE));

  private static JobKeyConverter jobKeyConv = new JobKeyConverter();

  /**
   * Used to connect to HBase.
   */
  private Connection hbaseConnection = null;

  /**
   * Used to create secondary index.
   */
  private JobHistoryByIdService jobHistoryByIdService = null;

  /**
   * Used to keep track of all the versions of the app we have seen.
   */
  private AppVersionService appVersionService = null;

  /**
   * Used to store raw blobs of job history and job conf
   */
  private JobHistoryRawService rawService = null;

  /**
   * Used to store aggregations of this job
   */
  private AppSummaryService appSummaryService = null;

  private long keyCount = 0;

  /**
   * determines whether or not to store aggregate stats
   */
  private boolean aggregationFlag = false;

  /**
   * determines whether or not to re-aggregate stats
   */
  private boolean reAggregationFlag = false;

  /**
   * @return the key class for the job output data.
   */
  public static Class getOutputKeyClass() {
    return ImmutableBytesWritable.class;
  }

  /**
   * @return the value class for the job output data.
   */
  public static Class getOutputValueClass() {
    return Put.class;
  }

  @Override
  protected void setup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {
    Configuration myConf = context.getConfiguration();
    hbaseConnection = ConnectionFactory.createConnection(myConf);
    jobHistoryByIdService = new JobHistoryByIdService(hbaseConnection);
    appVersionService = new AppVersionService(hbaseConnection);
    rawService = new JobHistoryRawService(hbaseConnection);
    // set aggregation to false by default
    aggregationFlag =
        myConf.getBoolean(AggregationConstants.AGGREGATION_FLAG_NAME, false);
    if (!aggregationFlag) {
      LOG.info("Aggregation is turned off ");
    }
    reAggregationFlag =
        myConf.getBoolean(AggregationConstants.RE_AGGREGATION_FLAG_NAME, false);
    if (reAggregationFlag) {
      LOG.info("Re-aggregation is turned ON, will be aggregating stats again "
          + " for jobs even if already aggregated status is true in raw table ");
    }
    appSummaryService = new AppSummaryService(hbaseConnection);

    keyCount = 0;
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result value,
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {

    keyCount++;
    boolean success = true;
    QualifiedJobId qualifiedJobId = null;
    JobDetails jobDetails = null;
    try {
      qualifiedJobId = rawService.getQualifiedJobIdFromResult(value);
      context.progress();

      Configuration jobConf = rawService.createConfigurationFromResult(value);
      context.progress();

      byte[] jobhistoryraw = rawService.getJobHistoryRawFromResult(value);
      long submitTimeMillis = JobHistoryFileParserBase
          .getSubmitTimeMillisFromJobHistory(jobhistoryraw);
      context.progress();
      // explicitly setting the byte array to null to free up memory
      jobhistoryraw = null;

      if (submitTimeMillis == 0L) {
        LOG.info(
            "NOTE: Since submitTimeMillis from job history is 0, now attempting to "
                + "approximate job start time based on last modification time from the raw table");
        // get an approximate submit time based on job history file's last
        // modification time
        submitTimeMillis = rawService.getApproxSubmitTime(value);
        context.progress();
      }

      Put submitTimePut =
          rawService.getJobSubmitTimePut(value.getRow(), submitTimeMillis);
      context.write(RAW_TABLE, submitTimePut);

      JobDesc jobDesc = JobDescFactory.createJobDesc(qualifiedJobId,
          submitTimeMillis, jobConf);
      JobKey jobKey = new JobKey(jobDesc);
      context.progress();

      // TODO: remove sysout
      String msg = "JobDesc (" + keyCount + "): " + jobDesc
          + " submitTimeMillis: " + submitTimeMillis;
      LOG.info(msg);

      List<Put> puts = JobHistoryService.getHbasePuts(jobDesc, jobConf);

      LOG.info("Writing " + puts.size() + " JobConf puts to "
          + Constants.HISTORY_TABLE);

      // TODO:
      // For Scalding just convert the flowID as a Hex number. Use that for the
      // runID.
      // Then do a post-processing step to re-write scalding flows. Rewrite
      // rows.
      // Scan should get the first (lowest job-id) then grab the start-time from
      // the Job.

      // Emit the puts
      for (Put put : puts) {
        context.write(JOB_TABLE, put);
        context.progress();
      }

      // Write secondary index(es)
      LOG.info("Writing secondary indexes");
      jobHistoryByIdService.writeIndexes(jobKey);
      context.progress();
      appVersionService.addVersion(jobDesc.getCluster(), jobDesc.getUserName(),
          jobDesc.getAppId(), jobDesc.getVersion(), jobDesc.getRunId());
      context.progress();

      KeyValue keyValue = value.getColumnLatest(Constants.RAW_FAM_BYTES,
          Constants.JOBHISTORY_COL_BYTES);

      byte[] historyFileContents = null;
      if (keyValue == null) {
        throw new MissingColumnInResultException(Constants.RAW_FAM_BYTES,
            Constants.JOBHISTORY_COL_BYTES);
      } else {
        historyFileContents = keyValue.getValue();
      }
      JobHistoryFileParser historyFileParser = JobHistoryFileParserFactory
          .createJobHistoryFileParser(historyFileContents, jobConf);

      historyFileParser.parse(historyFileContents, jobKey);
      context.progress();
      // set the byte array to null to help free up memory sooner
      historyFileContents = null;

      puts = historyFileParser.getJobPuts();
      if (puts == null) {
        throw new ProcessingException(
            " Unable to get job puts for this record!" + jobKey);
      }
      LOG.info(
          "Writing " + puts.size() + " Job puts to " + Constants.HISTORY_TABLE);

      // Emit the puts
      for (Put put : puts) {
        context.write(JOB_TABLE, put);
        // TODO: we should not have to do this, but need to confirm that
        // TableRecordWriter does this for us.
        context.progress();
      }

      puts = historyFileParser.getTaskPuts();
      if (puts == null) {
        throw new ProcessingException(
            " Unable to get task puts for this record!" + jobKey);
      }
      LOG.info("Writing " + puts.size() + " Job puts to "
          + Constants.HISTORY_TASK_TABLE);

      for (Put put : puts) {
        context.write(TASK_TABLE, put);
        // TODO: we should not have to do this, but need to confirm that
        // TableRecordWriter does this for us.
        context.progress();
      }

      /** post processing steps on job puts and job conf puts */
      Long mbMillis = historyFileParser.getMegaByteMillis();
      context.progress();
      if (mbMillis == null) {
        throw new ProcessingException(
            " Unable to get megabyte millis calculation for this record!"
                + jobKey);
      }

      Put mbPut = getMegaByteMillisPut(mbMillis, jobKey);
      LOG.info("Writing mega byte millis  puts to " + Constants.HISTORY_TABLE);
      context.write(JOB_TABLE, mbPut);
      context.progress();

      /** post processing steps to get cost of the job */
      Double jobCost = getJobCost(mbMillis, context.getConfiguration());
      context.progress();
      if (jobCost == null) {
        throw new ProcessingException(
            " Unable to get job cost calculation for this record!" + jobKey);
      }
      Put jobCostPut = getJobCostPut(jobCost, jobKey);
      LOG.info("Writing jobCost puts to " + Constants.HISTORY_TABLE);
      context.write(JOB_TABLE, jobCostPut);
      context.progress();

      jobDetails = historyFileParser.getJobDetails();
      if (jobDetails != null) {
        jobDetails.setCost(jobCost);
        jobDetails.setMegabyteMillis(mbMillis);
        jobDetails.setSubmitTime(jobKey.getRunId());
        jobDetails.setQueue(HadoopConfUtil.getQueueName(jobConf));
      }

    } catch (RowKeyParseException rkpe) {
      LOG.error(
          "Failed to process record "
              + (qualifiedJobId != null ? qualifiedJobId.toString() : ""),
          rkpe);
      success = false;
    } catch (MissingColumnInResultException mcire) {
      LOG.error(
          "Failed to process record "
              + (qualifiedJobId != null ? qualifiedJobId.toString() : ""),
          mcire);
      success = false;
    } catch (ProcessingException pe) {
      LOG.error("Failed to process record "
          + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), pe);
      success = false;
    } catch (IllegalArgumentException iae) {
      LOG.error("Failed to process record "
          + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), iae);
      success = false;
    }

    if (success) {
      // Update counter to indicate failure.
      HadoopCompat.incrementCounter(
          context.getCounter(ProcessingCounter.RAW_ROW_SUCCESS_COUNT), 1);
    } else {
      // Update counter to indicate failure.
      HadoopCompat.incrementCounter(
          context.getCounter(ProcessingCounter.RAW_ROW_ERROR_COUNT), 1);
    }

    // Indicate that we processed the RAW successfully so that we can skip it
    // on the next scan (or not).
    Put successPut =
        rawService.getJobProcessedSuccessPut(value.getRow(), success);
    // TODO: In the unlikely event of multiple mappers running against one RAW
    // row, with one succeeding and one failing, there could be a race where the
    // raw does not properly indicate the true status (which is questionable in
    // any case with multiple simultaneous runs with different outcome).
    context.write(RAW_TABLE, successPut);

    // consider aggregating job details
    if (jobDetails != null) {
      byte[] rowKey = value.getRow();
      if ((reAggregationFlag)
          || ((aggregationFlag) && (!rawService.getStatusAgg(rowKey,
              AggregationConstants.JOB_DAILY_AGGREGATION_STATUS_COL_BYTES)))) {
        aggreagteJobStats(jobDetails, rowKey, context,
            AggregationConstants.AGGREGATION_TYPE.DAILY);
        aggreagteJobStats(jobDetails, rowKey, context,
            AggregationConstants.AGGREGATION_TYPE.WEEKLY);
      } else {
        LOG.info("Not aggregating job info for " + jobDetails.getJobId());
      }
    }
  }

  /**
   * aggregate this job's stats only if re-aggregation is turned on OR
   * aggreation is on AND job not already aggregated
   *
   * if job has already been aggregated, we don't want to mistakenly aggregate
   * again
   */
  private void aggreagteJobStats(JobDetails jobDetails, byte[] rowKey,
      Context context, AggregationConstants.AGGREGATION_TYPE aggType)
      throws IOException, InterruptedException {

    byte[] aggStatusCol = null;
    switch (aggType) {
    case DAILY:
      aggStatusCol =
          AggregationConstants.JOB_DAILY_AGGREGATION_STATUS_COL_BYTES;
      break;
    case WEEKLY:
      aggStatusCol =
          AggregationConstants.JOB_WEEKLY_AGGREGATION_STATUS_COL_BYTES;
      break;
    default:
      LOG.error("Unknown aggregation type " + aggType);
      return;
    }

    boolean aggStatus =
        appSummaryService.aggregateJobDetails(jobDetails, aggType);
    context.progress();
    LOG.debug("Status of aggreagting stats for " + aggType + "=" + aggStatus);
    if (aggStatus) {
      // update raw table for this history file with aggregation status
      // Indicate that we processed the agg for this RAW successfully
      // so that we can skip it on the next scan (or not).
      Put aggStatusPut =
          rawService.getAggregatedStatusPut(rowKey, aggStatusCol, aggStatus);
      // TODO
      // In the unlikely event of multiple mappers running against one RAW
      // row, with one succeeding and one failing,
      // there could be a race where the
      // raw does not properly indicate the true status
      // (which is questionable in
      // any case with multiple simultaneous runs with different outcome).
      context.write(RAW_TABLE, aggStatusPut);
    }
  }

  /**
   * generates a put for the megabytemillis
   * @param mbMillis
   * @param jobKey
   * @return the put with megabytemillis
   */
  private Put getMegaByteMillisPut(Long mbMillis, JobKey jobKey) {
    Put pMb = new Put(jobKeyConv.toBytes(jobKey));
    pMb.addColumn(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES,
        Bytes.toBytes(mbMillis));
    return pMb;
  }

  /**
   * looks for cost file in distributed cache
   * @param cachePath of the cost properties file
   * @param machineType of the node the job ran on
   * @throws IOException
   */
  Properties loadCostProperties(Path cachePath, String machineType) {
    Properties prop = new Properties();
    InputStream inp = null;
    try {
      inp = new FileInputStream(cachePath.toString());
      prop.load(inp);
      return prop;
    } catch (FileNotFoundException fnf) {
      LOG.error("cost properties does not exist, using default values");
      return null;
    } catch (IOException e) {
      LOG.error("error loading properties, using default values");
      return null;
    } finally {
      if (inp != null) {
        try {
          inp.close();
        } catch (IOException ignore) {
          // do nothing
        }
      }
    }
  }

  /**
   * calculates the cost of this job based on mbMillis, machineType and cost
   * details from the properties file
   * @param mbMillis
   * @param currentConf
   * @return cost of the job
   */
  private Double getJobCost(Long mbMillis, Configuration currentConf) {
    Double computeTco = 0.0;
    Long machineMemory = 0L;
    Properties prop = null;
    String machineType =
        currentConf.get(Constants.HRAVEN_MACHINE_TYPE, "default");
    LOG.debug(" machine type " + machineType);
    try {
      Path[] cacheFiles = DistributedCache.getLocalCacheFiles(currentConf);
      if (null != cacheFiles && cacheFiles.length > 0) {
        for (Path cachePath : cacheFiles) {
          LOG.debug(" distributed cache path " + cachePath);
          if (cachePath.getName().equals(Constants.COST_PROPERTIES_FILENAME)) {
            prop = loadCostProperties(cachePath, machineType);
            break;
          }
        }
      } else {
        LOG.error(
            "Unable to find anything (" + Constants.COST_PROPERTIES_FILENAME
                + ") in distributed cache, continuing with defaults");
      }

    } catch (IOException ioe) {
      LOG.error("IOException reading from distributed cache for "
          + Constants.COST_PROPERTIES_HDFS_DIR + ", continuing with defaults"
          + ioe.toString());
    }
    if (prop != null) {
      String computeTcoStr = prop.getProperty(machineType + ".computecost");
      try {
        computeTco = Double.parseDouble(computeTcoStr);
      } catch (NumberFormatException nfe) {
        LOG.error("error in conversion to long for compute tco " + computeTcoStr
            + " using default value of 0");
      }
      String machineMemStr = prop.getProperty(machineType + ".machinememory");
      try {
        machineMemory = Long.parseLong(machineMemStr);
      } catch (NumberFormatException nfe) {
        LOG.error("error in conversion to long for machine memory  "
            + machineMemStr + " using default value of 0");
      }
    } else {
      LOG.error("Could not load properties file, using defaults");
    }

    Double jobCost = JobHistoryFileParserBase.calculateJobCost(mbMillis,
        computeTco, machineMemory);
    LOG.info("from cost properties file, jobCost is " + jobCost
        + " based on compute tco: " + computeTco + " machine memory: "
        + machineMemory + " for machine type " + machineType);
    return jobCost;
  }

  /**
   * generates a put for the job cost
   * @param jobCost
   * @param jobKey
   * @return the put with job cost
   */
  private Put getJobCostPut(Double jobCost, JobKey jobKey) {
    Put pJobCost = new Put(jobKeyConv.toBytes(jobKey));
    pJobCost.addColumn(Constants.INFO_FAM_BYTES, Constants.JOBCOST_BYTES,
        Bytes.toBytes(jobCost));
    return pJobCost;
  }

  @Override
  protected void cleanup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {

    if (hbaseConnection != null) {
      hbaseConnection.close();
    }
  }
}
