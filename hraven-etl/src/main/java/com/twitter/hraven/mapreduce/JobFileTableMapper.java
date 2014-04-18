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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
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

/**
 * Takes in results from a scan from {@link ProcessRecordService
 * @getHistoryRawTableScan}, process both job file and history file and emit out
 * as puts for the {@link Constants#HISTORY_TABLE}
 * <p>
 * As a side-affect we'll load an index record into the
 * {@link Constants#HISTORY_BY_JOBID_TABLE} as well.
 * 
 */
public class JobFileTableMapper extends
    TableMapper<ImmutableBytesWritable, Put> {

  private static Log LOG = LogFactory.getLog(JobFileTableMapper.class);

  private static final ImmutableBytesWritable JOB_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_TABLE_BYTES);
  private static final ImmutableBytesWritable TASK_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_TASK_TABLE_BYTES);
  private static final ImmutableBytesWritable RAW_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_RAW_TABLE_BYTES);
  private static JobKeyConverter jobKeyConv = new JobKeyConverter();

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

  private long keyCount = 0;

  /**
   * @return the key class for the job output data.
   */
  public static Class<? extends WritableComparable<ImmutableBytesWritable>> getOutputKeyClass() {
    return ImmutableBytesWritable.class;
  }

  /**
   * @return the value class for the job output data.
   */
  public static Class<? extends Writable> getOutputValueClass() {
    return Put.class;
  }

  @Override
  protected void setup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {
    Configuration myConf = context.getConfiguration();
    jobHistoryByIdService = new JobHistoryByIdService(myConf);
    appVersionService = new AppVersionService(myConf);
    rawService = new JobHistoryRawService(myConf);

    keyCount = 0;
  }

  @Override
  protected void map(
      ImmutableBytesWritable key,
      Result value,
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {

    keyCount++;
    boolean success = true;
    QualifiedJobId qualifiedJobId = null;
    try {
      qualifiedJobId = rawService.getQualifiedJobIdFromResult(value);
      context.progress();

      Configuration jobConf = rawService.createConfigurationFromResult(value);
      context.progress();

      long submitTimeMillis = rawService.getSubmitTimeMillisFromResult(value);
      context.progress();

      Put submitTimePut = rawService.getJobSubmitTimePut(value.getRow(),
          submitTimeMillis);
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
          jobDesc.getAppId(), jobDesc.getVersion(), submitTimeMillis);
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

      puts = historyFileParser.getJobPuts();
      if (puts == null) {
    	  throw new ProcessingException(
    			  " Unable to get job puts for this record!" + jobKey);
      }
      LOG.info("Writing " + puts.size() + " Job puts to "
          + Constants.HISTORY_TABLE);

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
        throw new ProcessingException(" Unable to get megabyte millis calculation for this record!"
              + jobKey);
      }

      Put mbPut = getMegaByteMillisPut(mbMillis, jobKey);
      LOG.info("Writing mega byte millis  puts to " + Constants.HISTORY_TABLE);
      context.write(JOB_TABLE, mbPut);
      context.progress();

      /** post processing steps to get cost of the job */
      Double jobCost = getJobCost( mbMillis, context.getConfiguration());
      context.progress();
      if (jobCost == null) {
        throw new ProcessingException(" Unable to get job cost calculation for this record!"
              + jobKey);
      }

      Put jobCostPut = getJobCostPut(jobCost, jobKey);
      LOG.info("Writing jobCost puts to " + Constants.HISTORY_TABLE);
      context.write(JOB_TABLE, jobCostPut);
      context.progress();

    } catch (RowKeyParseException rkpe) {
      LOG.error("Failed to process record "
          + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), rkpe);
      success = false;
    } catch (MissingColumnInResultException mcire) {
      LOG.error("Failed to process record "
          + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), mcire);
      success = false;
    } catch (ProcessingException pe) {
      LOG.error("Failed to process record "
          + (qualifiedJobId != null ? qualifiedJobId.toString() : ""), pe);
      success = false;
    } catch (IllegalArgumentException iae) {
      LOG.error("Failed to process record "
              + (qualifiedJobId != null ? qualifiedJobId.toString() : ""),iae);
      success = false;
    }

    if (success) {
      // Update counter to indicate failure.
      HadoopCompat.incrementCounter(context.getCounter(ProcessingCounter.RAW_ROW_SUCCESS_COUNT), 1);
    } else {
      // Update counter to indicate failure.
      HadoopCompat.incrementCounter(context.getCounter(ProcessingCounter.RAW_ROW_ERROR_COUNT),1);
    }

    // Indicate that we processed the RAW successfully so that we can skip it
    // on the next scan (or not).
    Put successPut = rawService.getJobProcessedSuccessPut(value.getRow(),
        success);
    // TODO: In the unlikely event of multiple mappers running against one RAW
    // row, with one succeeding and one failing, there could be a race where the
    // raw does not properly indicate the true status (which is questionable in
    // any case with multiple simultaneous runs with different outcome).
    context.write(RAW_TABLE, successPut);

  }

  /**
   * looks for cost file in distributed cache
   * @param cachePath of the cost properties file
   * @param machineType of the node the job ran on
   */
  Properties loadCostProperties(Path cachePath, String machineType) {
    Properties prop = new Properties();
    try {
      InputStream inp = new FileInputStream(cachePath.toString());
      prop.load(inp);
      return prop;
    } catch (FileNotFoundException fnf) {
      LOG.error("cost properties does not exist, using default values");
      return null;
    } catch (IOException e) {
      LOG.error("error loading properties, using default values");
      return null;
    }
  }

  /**
   * calculates the cost of this job based on mbMillis, machineType and
   * cost details from the properties file
   * @param mbMillis
   * @param currentConf
   * @return cost of the job
   */
  private Double getJobCost(Long mbMillis, Configuration currentConf) {
    Double computeTco = Constants.DEFAULT_COMPUTE_TCO;
    Long machineMemory = Constants.DEFAULT_MACHINE_MEMORY;
    Properties prop = null;
    String machineType = currentConf.get(Constants.HRAVEN_MACHINE_TYPE, "default");
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
        LOG.error("Unable to find anything (" + Constants.COST_PROPERTIES_FILENAME
            + ") in distributed cache, continuing with defaults");
      }
    } catch (IOException ioe) {
      LOG.error("IOException reading from distributed cache for "
          + Constants.COST_PROPERTIES_HDFS_DIR + ", continuing with defaults" + ioe.toString());
    }

    if (prop != null) {
      String computeTcoStr = prop.getProperty(machineType + ".computecost");
      try {
        computeTco = Double.parseDouble(computeTcoStr);
      } catch (NumberFormatException nfe) {
        LOG.error("error in conversion to long for compute tco " + computeTcoStr
            + " using default " + Constants.DEFAULT_COMPUTE_TCO);
      }
      String machineMemStr = prop.getProperty(machineType + ".machinememory");
      try {
        machineMemory = Long.parseLong(machineMemStr);
      } catch (NumberFormatException nfe) {
        LOG.error("error in conversion to long for machine memory  " + machineMemStr
            + " using default " + Constants.DEFAULT_MACHINE_MEMORY);
      }
    } else {
      LOG.error("Could not load properties file, using defaults");
    }

    Double jobCost = JobHistoryFileParserBase.calculateJobCost(mbMillis, computeTco, machineMemory);
    LOG.info("from cost properties file, jobCost is " + jobCost + " based on compute tco: "
        + computeTco + " machine memory: " + machineMemory + " for machine type " + machineType);

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
    pJobCost.add(Constants.INFO_FAM_BYTES, Constants.JOBCOST_BYTES, Bytes.toBytes(jobCost));
    return pJobCost;
  }

  /**
   * generates a put for the megabytemillis
   * @param mbMillis
   * @param jobKey
   * @return the put with megabytemillis
   */
  private Put getMegaByteMillisPut(Long mbMillis, JobKey jobKey) {
    Put pMb = new Put(jobKeyConv.toBytes(jobKey));
    pMb.add(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(mbMillis));
    return pMb;
  }

  @Override
  protected void cleanup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws java.io.IOException, InterruptedException {

    IOException caught = null;

    if (jobHistoryByIdService != null) {
      try {
        jobHistoryByIdService.close();
      } catch (IOException ioe) {
        caught = ioe;
      }
    }
    if (appVersionService != null) {
      try {
        appVersionService.close();
      } catch (IOException ioe) {
        // TODO: don't overwrite a previous exception
        caught = ioe;
      }
    }
    if (rawService != null) {
      try {
        rawService.close();
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
