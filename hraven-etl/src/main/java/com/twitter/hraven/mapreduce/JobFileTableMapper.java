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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobHistoryTaskRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
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
import com.twitter.hraven.etl.Sink;
import com.twitter.hraven.util.EnumWritable;

/**
 * Takes in results from a scan from {@link ProcessRecordService
 * @getHistoryRawTableScan}, process both job file and history file and emit out
 * as {@link HravenService}, {@link HravenRecord}. {@link Sink}s will process these
 * and convert to their respective storage formats
 * <p>
 * As a side-affect we'll load an index record into the
 * {@link Constants#HISTORY_BY_JOBID_TABLE} as well.
 * 
 */
public class JobFileTableMapper extends
    TableMapper<HravenService, HravenRecord> {

  private static Log LOG = LogFactory.getLog(JobFileTableMapper.class);

  private static final ImmutableBytesWritable JOB_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_TABLE_BYTES);
  private static final ImmutableBytesWritable TASK_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_TASK_TABLE_BYTES);
  private static final ImmutableBytesWritable RAW_TABLE = new ImmutableBytesWritable(
      Constants.HISTORY_RAW_TABLE_BYTES);
  private static JobKeyConverter jobKeyConv = new JobKeyConverter();

  private MultipleOutputs mos;
  
  /**
   *  Define the sinks to output to 
   */
  
  private ArrayList<Sink> sinks;
  
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
  public static Class<?> getOutputKeyClass() {
    return EnumWritable.class;
  }

  /**
   * @return the value class for the job output data.
   */
  public static Class<?> getOutputValueClass() {
    return HravenRecord.class;
  }

  @Override
  protected void setup(
      Mapper<ImmutableBytesWritable, Result, HravenService, HravenRecord>.Context context)
      throws java.io.IOException, InterruptedException {
    Configuration myConf = context.getConfiguration();
    jobHistoryByIdService = new JobHistoryByIdService(myConf);
    appVersionService = new AppVersionService(myConf);
    rawService = new JobHistoryRawService(myConf);
    keyCount = 0;

    sinks =
        new ArrayList<Sink>(Collections2.transform(Arrays.asList(StringUtils.split(context
            .getConfiguration().get(Constants.JOBCONF_SINKS), ',')), new Function<String, Sink>() {

          @Override
          @Nullable
          public Sink apply(@Nullable String input) {
            return Sink.valueOf(input);
          }
        }));

    mos = new MultipleOutputs<HravenService, HravenRecord>(context);
  }

  @Override
  protected void map(
      ImmutableBytesWritable key,
      Result value,
      Mapper<ImmutableBytesWritable, Result, HravenService, HravenRecord>.Context context)
      throws java.io.IOException, InterruptedException {

    keyCount++;
    boolean success = true;
    QualifiedJobId qualifiedJobId = null;
    try {
    	
    	
      /**
       *  STEP 0
       *  
       *  init and extract meaningful data out of raw value
       *  
       *   **/
    	
      qualifiedJobId = rawService.getQualifiedJobIdFromResult(value);
      context.progress();

      Configuration jobConf = rawService.createConfigurationFromResult(value);
      context.progress();

      //figure out submit time
      byte[] jobhistoryraw = rawService.getJobHistoryRawFromResult(value);
      long submitTimeMillis = JobHistoryFileParserBase.getSubmitTimeMillisFromJobHistory(
            jobhistoryraw);
      context.progress();

      if (submitTimeMillis == 0L) {
        LOG.info("NOTE: Since submitTimeMillis from job history is 0, now attempting to "
            + "approximate job start time based on last modification time from the raw table");
        // get an approximate submit time based on job history file's last modification time
        submitTimeMillis = rawService.getApproxSubmitTime(value);
        context.progress();
      }

      Put submitTimePut = rawService.getJobSubmitTimePut(value.getRow(),
          submitTimeMillis);
      
      rawService.getTable().put(submitTimePut);
      
      /** 
       * STEP 1
       * 
       * process and extract job xml/conf
       * 
       * **/
      JobDesc jobDesc = JobDescFactory.createJobDesc(qualifiedJobId,
          submitTimeMillis, jobConf);
      JobKey jobKey = new JobKey(jobDesc);
      context.progress();
      
      // TODO: remove sysout
      String msg = "JobDesc (" + keyCount + "): " + jobDesc
          + " submitTimeMillis: " + submitTimeMillis;
      LOG.info(msg);

      JobHistoryRecordCollection confRecordCollection = JobHistoryService.getConfRecord(jobDesc, jobConf);

      LOG.info("Sending JobConf records to "
          + HravenService.JOB_HISTORY + " service");

      // TODO:
      // For Scalding just convert the flowID as a Hex number. Use that for the
      // runID.
      // Then do a post-processing step to re-write scalding flows. Rewrite
      // rows.
      // Scan should get the first (lowest job-id) then grab the start-time from
      // the Job.

      //1.1 Emit the records for job xml/conf/"JobDesc"
      //Don't sink config seperately - merge with all other records and then sink
      //sink(HravenService.JOB_HISTORY, confRecord);
      context.progress();

      /** 
       * STEP 2
       * 
       * Write secondary index(es)
       * 
       * **/
      
      LOG.info("Writing secondary indexes");
      jobHistoryByIdService.writeIndexes(jobKey);
      context.progress();
      appVersionService.addVersion(jobDesc.getCluster(), jobDesc.getUserName(),
          jobDesc.getAppId(), jobDesc.getVersion(), jobDesc.getRunId());
      context.progress();

      /** 
       * STEP 3
       * 
       * Process and extact actual job history
       * 
       * **/
      
      //3.1: get job history
      KeyValue keyValue = value.getColumnLatest(Constants.RAW_FAM_BYTES,
       Constants.JOBHISTORY_COL_BYTES);

      byte[] historyFileContents = null;
      if (keyValue == null) {
        throw new MissingColumnInResultException(Constants.RAW_FAM_BYTES,
          Constants.JOBHISTORY_COL_BYTES);
      } else {
        historyFileContents = keyValue.getValue();
      }
      
      //3.2: parse job history
      JobHistoryFileParser historyFileParser = JobHistoryFileParserFactory
    		  .createJobHistoryFileParser(historyFileContents, jobConf);

      historyFileParser.parse(historyFileContents, jobKey);
      context.progress();

      //3.3: get and write job related data
      JobHistoryRecordCollection jobHistoryRecords = (JobHistoryRecordCollection) historyFileParser.getJobRecords();
      jobHistoryRecords.setSubmitTime(submitTimeMillis);
      jobHistoryRecords.mergeWith(confRecordCollection);
      
      LOG.info("Sending " + jobHistoryRecords.size() + " Job history records to "
          + HravenService.JOB_HISTORY + " service");
      
      context.progress();

      //3.4: post processing steps on job records and job conf records
      Long mbMillis = historyFileParser.getMegaByteMillis();
      context.progress();
      if (mbMillis == null) {
        throw new ProcessingException(" Unable to get megabyte millis calculation for this record!"
              + jobKey);
      }

      JobHistoryRecord mbRecord = getMegaByteMillisRecord(mbMillis, jobKey);
      LOG.info("Send mega byte millis records to " + HravenService.JOB_HISTORY + " service");
      
      jobHistoryRecords.add(mbRecord);
      context.progress();

      //3.5: post processing steps to get cost of the job */
      Double jobCost = getJobCost(mbMillis, context.getConfiguration());
      context.progress();
      if (jobCost == null) {
        throw new ProcessingException(" Unable to get job cost calculation for this record!"
            + jobKey);
      }
      JobHistoryRecord jobCostRecord = getJobCostRecord(jobCost, jobKey);
      LOG.info("Send jobCost records to " + HravenService.JOB_HISTORY + " service");
      jobHistoryRecords.add(jobCostRecord);
      
      //Sink the merged record
      sink(HravenService.JOB_HISTORY, jobHistoryRecords);
      context.progress();
      
      //3.6: get and write task related data
      ArrayList<JobHistoryTaskRecord> taskHistoryRecords = (ArrayList<JobHistoryTaskRecord>) historyFileParser.getTaskRecords();
      
      LOG.info("Sending " + taskHistoryRecords.size() + " Task history records to "
          + HravenService.JOB_HISTORY_TASK + " service");

      for (JobHistoryTaskRecord taskRecord: taskHistoryRecords) {
        sink(HravenService.JOB_HISTORY_TASK, taskRecord);  
      }
      
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

    /**
     * STEP 4
     * 
     * Finalization
     * 
     * **/
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
    rawService.getTable().put(successPut);
  }

	private void sink(HravenService service, HravenRecord record)
			throws IOException, InterruptedException {
		
	  for (Sink sink: sinks) {
	    mos.write(sink.name(), new EnumWritable(service), record);
	  }
	}

/**
   * generates a record for the megabytemillis
   * @param mbMillis
   * @param jobKey
   * @return the record with megabytemillis
   */
	private JobHistoryRecord getMegaByteMillisRecord(Long mbMillis,
			JobKey jobKey) {
		return new JobHistoryRecord(RecordCategory.INFERRED, jobKey,
				new RecordDataKey(Constants.MEGABYTEMILLIS), mbMillis);
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
   * calculates the cost of this job based on mbMillis, machineType
   * and cost details from the properties file
   * @param mbMillis
   * @param currentConf
   * @return cost of the job
   */
  private Double getJobCost(Long mbMillis, Configuration currentConf) {
    Double computeTco = 0.0;
    Long machineMemory = 0L;
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
            + " using default value of 0");
      }
      String machineMemStr = prop.getProperty(machineType + ".machinememory");
      try {
        machineMemory = Long.parseLong(machineMemStr);
      } catch (NumberFormatException nfe) {
        LOG.error("error in conversion to long for machine memory  " + machineMemStr
            + " using default value of 0");
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
   * generates a record for the job cost
   * @param jobCost
   * @param jobKey
   * @return the record with job cost
   */
  
  private JobHistoryRecord getJobCostRecord(Double jobCost, JobKey jobKey) {
	return new JobHistoryRecord(RecordCategory.INFERRED, jobKey,
			new RecordDataKey(Constants.JOBCOST), jobCost);
  }

  @Override
  protected void cleanup(
      Mapper<ImmutableBytesWritable, Result, HravenService, HravenRecord>.Context context)
      throws java.io.IOException, InterruptedException {

    IOException caught = null;
    
    //Close the MultipleOutputs instance
    //to call close on all wrapped OutputFormats's RecordWriters
    mos.close();
    
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
