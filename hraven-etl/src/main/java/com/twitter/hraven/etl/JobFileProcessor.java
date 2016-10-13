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

import static com.twitter.hraven.etl.ProcessState.LOADED;
import static com.twitter.hraven.etl.ProcessState.PROCESSED;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.twitter.hraven.AggregationConstants;
import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.etl.ProcessRecordService;
import com.twitter.hraven.datasource.RowKeyParseException;
import com.twitter.hraven.mapreduce.JobFileTableMapper;

/**
 * Used to process one ProcessingRecord at at time. For each record an HBase job
 * is created to scan the corresponding rows in the raw
 * 
 */
public class JobFileProcessor extends Configured implements Tool {

  final static String NAME = JobFileProcessor.class.getSimpleName();
  private static Log LOG = LogFactory.getLog(JobFileProcessor.class);

  private final String startTimestamp = Constants.TIMESTAMP_FORMAT
      .format(new Date(System.currentTimeMillis()));

  private final AtomicInteger jobCounter = new AtomicInteger(0);

  /**
   * Maximum number of files to process in one batch.
   */
  private final static int DEFAULT_BATCH_SIZE = 100;

  /**
   * Default constructor.
   */
  public JobFileProcessor() {

  }

  /**
   * Used for injecting confs while unit testing
   * 
   * @param conf
   */
  public JobFileProcessor(Configuration conf) {
    super(conf);

  }

  /**
   * Parse command-line arguments.
   * 
   * @param args
   *          command line arguments passed to program.
   * @return parsed command line.
   * @throws ParseException
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();

    // Input
    Option o = new Option("c", "cluster", true,
        "cluster for which jobs are processed");
    o.setArgName("cluster");
    o.setRequired(true);
    options.addOption(o);

    // Whether to skip existing files or not.
    o = new Option(
        "r",
        "reprocess",
        false,
        "Reprocess only those records that have been marked to be reprocessed. Otherwise process all rows indicated in the processing records, but successfully processed job files are skipped.");
    o.setRequired(false);
    options.addOption(o);

    // Whether to aggregate or not.
    // if re-process is on, need to consider turning aggregation off
    o = new Option(
        "a",
        "aggregate",
        true,
        "Whether to aggreagate job details or not.");
    o.setArgName("aggreagte");
    o.setRequired(false);
    options.addOption(o);

    // Whether to force re-aggregation or not.
    o = new Option(
        "ra",
        "re-aggregate",
        true,
        "Whether to re-aggreagate job details or not.");
    o.setArgName("re-aggreagte");
    o.setRequired(false);
    options.addOption(o);

    // Batch
    o = new Option("b", "batchSize", true,
        "The number of files to process in one batch. Default "
            + DEFAULT_BATCH_SIZE);
    o.setArgName("batch-size");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(
        "t",
        "threads",
        true,
        "Number of parallel threads to use to run Hadoop jobs simultaniously. Default = 1");
    o.setArgName("thread-count");
    o.setRequired(false);
    options.addOption(o);

    o = new Option(
        "p",
        "processFileSubstring",
        true,
        "use only those process records where the process file path contains the provided string. Useful when processing production jobs in parallel to historic loads.");
    o.setArgName("processFileSubstring");
    o.setRequired(false);
    options.addOption(o);

    // Debugging
    options.addOption("d", "debug", false, "switch on DEBUG log level");

    o = new Option("zf", "costFile", true, "The cost properties file location on HDFS");
    o.setArgName("costfile_loc");
    o.setRequired(true);
    options.addOption(o);

    // Machine type
    o = new Option("m", "machineType", true,
      "The type of machine this job ran on");
    o.setArgName("machinetype");
    o.setRequired(true);
    options.addOption(o);

    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }

    // Set debug level right away
    if (commandLine.hasOption("d")) {
      Logger log = Logger.getLogger(JobFileProcessor.class);
      log.setLevel(Level.DEBUG);
    }

    return commandLine;

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  public int run(String[] args) throws Exception {

    Configuration hbaseConf = HBaseConfiguration.create(getConf());

    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs = new GenericOptionsParser(hbaseConf, args)
        .getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    // Grab the cluster argument
    String cluster = commandLine.getOptionValue("c");
    LOG.info("cluster=" + cluster);

    // Number of parallel threads to use
    int threadCount = 1;
    if (commandLine.hasOption("t")) {
      try {
        threadCount = Integer.parseInt(commandLine.getOptionValue("t"));
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "Provided thread-count argument (-t) is not a number: "
                + commandLine.getOptionValue("t"), nfe);
      }
      if (threadCount < 1) {
        throw new IllegalArgumentException(
            "Cannot run fewer than 1 thread. Provided thread-count argument (-t): "
                + threadCount);
      }
    }
    LOG.info("threadCount=" + threadCount);

    boolean reprocess = commandLine.hasOption("r");
    LOG.info("reprocess=" + reprocess);

    // Grab the batch-size argument
    int batchSize;
    if (commandLine.hasOption("b")) {
      try {
        batchSize = Integer.parseInt(commandLine.getOptionValue("b"));
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "batch size option -b is is not a valid number: "
                + commandLine.getOptionValue("b"), nfe);
      }
      // Additional check
      if (batchSize < 1) {
        throw new IllegalArgumentException(
            "Cannot process files in batches smaller than 1. Specified batch size option -b is: "
                + commandLine.getOptionValue("b"));
      }
    } else {
      batchSize = DEFAULT_BATCH_SIZE;
    }

    // Grab the costfile argument

    String costFilePath = commandLine.getOptionValue("zf");
    LOG.info("cost properties file on hdfs=" + costFilePath);
    if (costFilePath == null) costFilePath = Constants.COST_PROPERTIES_HDFS_DIR;
    Path hdfsPath = new Path(costFilePath + Constants.COST_PROPERTIES_FILENAME);
    // add to distributed cache
    DistributedCache.addCacheFile(hdfsPath.toUri(), hbaseConf);
    
    // Grab the machine type argument
    String machineType = commandLine.getOptionValue("m");
    // set it as part of conf so that the
    // hRaven job can access it in the mapper
    hbaseConf.set(Constants.HRAVEN_MACHINE_TYPE, machineType);

    // check if re-aggregate option is forced on
    // if yes, we need to aggregate for this job inspite of
    // job having aggregation done status in raw table
    boolean reAggregateFlagValue = false;
    if (commandLine.hasOption("ra")) {
      String reaggregateFlag = commandLine.getOptionValue("ra");
      // set it as part of conf so that the
      // hRaven jobProcessor can access it in the mapper
      if (StringUtils.isNotBlank(reaggregateFlag)) {
        LOG.info(" reaggregateFlag is: " + reaggregateFlag);
        if (StringUtils.equalsIgnoreCase(reaggregateFlag, Boolean.TRUE.toString())) {
          reAggregateFlagValue = true;
        }
      }
    }
    LOG.info(AggregationConstants.RE_AGGREGATION_FLAG_NAME +"=" + reAggregateFlagValue);
    hbaseConf.setBoolean(AggregationConstants.RE_AGGREGATION_FLAG_NAME, reAggregateFlagValue);

    // set aggregation to off by default
    boolean aggFlagValue = false;
    if (commandLine.hasOption("a")) {
      String aggregateFlag = commandLine.getOptionValue("a");
      // set it as part of conf so that the
      // hRaven jobProcessor can access it in the mapper
      if (StringUtils.isNotBlank(aggregateFlag)) {
        LOG.info(" aggregateFlag is: " + aggregateFlag);
        if (StringUtils.equalsIgnoreCase(aggregateFlag, Boolean.TRUE.toString())) {
          aggFlagValue = true;
        } 
      }
    }
    if(reprocess) {
      // turn off aggregation if reprocessing is true
      // we don't want to inadvertently aggregate again while re-processing
      // re-aggregation needs to be a conscious setting
      aggFlagValue = false;
    }
    LOG.info(AggregationConstants.AGGREGATION_FLAG_NAME +"=" + aggFlagValue);
    hbaseConf.setBoolean(AggregationConstants.AGGREGATION_FLAG_NAME, aggFlagValue);

    String processFileSubstring = null;
    if (commandLine.hasOption("p")) {
      processFileSubstring = commandLine.getOptionValue("p");
    }
    LOG.info("processFileSubstring=" + processFileSubstring);

    // hbase.client.keyvalue.maxsize somehow defaults to 10 MB and we have
    // history files exceeding that. Disable limit.
    hbaseConf.setInt("hbase.client.keyvalue.maxsize", 0);

    // Shove this into the jobConf so that we can get it out on the task side.
    hbaseConf.setStrings(Constants.CLUSTER_JOB_CONF_KEY, cluster);

    boolean success = false;
    if (reprocess) {
      success = reProcessRecords(hbaseConf, cluster, batchSize, threadCount);
    } else {
      success = processRecords(hbaseConf, cluster, batchSize, threadCount,
          processFileSubstring);
    }

    // Return the status
    return success ? 0 : 1;
  }

  /**
   * Pick up the ranges of jobs to process from ProcessRecords. Skip raw rows
   * that have already been processed.
   * 
   * @param conf
   *          used to contact HBase and to run jobs against
   * @param cluster
   *          for which to process records.
   * @param batchSize
   *          the total number of jobs to process in a batch (a MR job scanning
   *          these many records in the raw table).
   * @param threadCount
   *          how many parallel threads should be used to run Hadoop jobs in
   *          parallel.
   * @param processFileSubstring
   *          Use only process records where the process file path contains this
   *          string. If <code>null</code> or empty string, then no filtering is
   *          applied.
   * @return whether all job files for all processRecords were properly
   *         processed.
   * @throws IOException
   * @throws ClassNotFoundException
   *           when problems occur setting up the job.
   * @throws InterruptedException
   * @throws ExecutionException
   *           when at least one of the jobs could not be scheduled.
   * @throws RowKeyParseException
   */
  boolean processRecords(Configuration conf, String cluster, int batchSize,
      int threadCount, String processFileSubstring) throws IOException,
      InterruptedException, ClassNotFoundException, ExecutionException,
      RowKeyParseException {

    List<ProcessRecord> processRecords = getProcessRecords(conf, cluster,
        processFileSubstring);

    // Bail out early if needed
    if ((processRecords == null) || (processRecords.size() == 0)) {
      return true;
    }

    // Grab the min and the max jobId from all processing records.
    MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();

    for (ProcessRecord processRecord : processRecords) {
      minMaxJobFileTracker.track(processRecord.getMinJobId());
      minMaxJobFileTracker.track(processRecord.getMaxJobId());
    }

    List<JobRunner> jobRunners = getJobRunners(conf, cluster, false, batchSize,
        minMaxJobFileTracker.getMinJobId(), minMaxJobFileTracker.getMaxJobId());

    boolean success = runJobs(threadCount, jobRunners);
    if (success) {
      updateProcessRecords(conf, processRecords);
    }

    return success;
  }

  /**
   * @param conf
   *          used to contact HBase and to run jobs against
   * @param cluster
   *          for which to process records.
   * @param batchSize
   *          the total number of jobs to process in a batch (a MR job scanning
   *          these many records in the raw table).
   * @param threadCount
   *          how many parallel threads should be used to run Hadoop jobs in
   *          parallel.
   * @return whether all job files for all processRecords were properly
   *         processed.
   * @throws IOException
   * @throws ClassNotFoundException
   *           when problems occur setting up the job.
   * @throws InterruptedException
   * @throws ExecutionException
   *           when at least one of the jobs could not be scheduled.
   * @throws RowKeyParseException
   */
  boolean reProcessRecords(Configuration conf, String cluster, int batchSize,
      int threadCount) throws IOException, InterruptedException,
      ClassNotFoundException, ExecutionException, RowKeyParseException {

    List<JobRunner> jobRunners = getJobRunners(conf, cluster, true, batchSize,
        null, null);

    boolean success = runJobs(threadCount, jobRunners);
    return success;
  }

  /**
   * Run the jobs and wait for all of them to complete.
   * 
   * @param threadCount
   *          up to how many jobs to run in parallel
   * @param jobRunners
   *          the list of jobs to run.
   * @return whether all jobs completed successfully or not.
   * @throws InterruptedException
   *           when interrupted while running jobs.
   * @throws ExecutionException
   *           when at least one of the jobs could not be scheduled.
   */
  private boolean runJobs(int threadCount, List<JobRunner> jobRunners)
      throws InterruptedException, ExecutionException {
    ExecutorService execSvc = Executors.newFixedThreadPool(threadCount);

    if ((jobRunners == null) || (jobRunners.size() == 0)) {
      return true;
    }

    boolean success = true;
    try {
      List<Future<Boolean>> jobFutures = new LinkedList<Future<Boolean>>();
      for (JobRunner jobRunner : jobRunners) {
        Future<Boolean> jobFuture = execSvc.submit(jobRunner);
        jobFutures.add(jobFuture);
      }

      // Wait for all jobs to complete.
      for (Future<Boolean> jobFuture : jobFutures) {
        success = jobFuture.get();
        if (!success) {
          // Stop the presses as soon as we see an error. Note that several
          // other jobs may have already been scheduled. Others will never be
          // scheduled.
          break;
        }
      }
    } finally {
      // Shut down the executor so that the JVM can exit.
      List<Runnable> neverRan = execSvc.shutdownNow();
      if (neverRan != null && neverRan.size() > 0) {
        System.err
            .println("Interrupted run. Currently running Hadoop jobs will continue unless cancelled. "
                + neverRan + " jobs never scheduled.");
      }
    }
    return success;
  }

  /**
   * @param conf
   *          to be used to connect to HBase
   * @param cluster
   *          for which we're finding processRecords.
   * @param processFileSubstring
   *          if specified, this string must be part of the processFile path to
   *          limit which records we want to process.
   * @return a list of processRecords in {@link ProcessState#LOADED} stqte that
   *         still need to be processed.
   * @throws IOException
   */
  private List<ProcessRecord> getProcessRecords(Configuration conf,
      String cluster, String processFileSubstring) throws IOException {
    ProcessRecordService processRecordService = new ProcessRecordService(conf);
    IOException caught = null;
    List<ProcessRecord> processRecords = null;
    try {
      // Grab all records.
      processRecords = processRecordService.getProcessRecords(cluster, LOADED,
          Integer.MAX_VALUE, processFileSubstring);

      LOG.info("Processing " + processRecords.size() + " for: " + cluster);
    } catch (IOException ioe) {
      caught = ioe;
    } finally {
      try {
        processRecordService.close();
      } catch (IOException ioe) {
        if (caught == null) {
          caught = ioe;
        }
      }
      if (caught != null) {
        throw caught;
      }
    }
    return processRecords;
  }

  /**
   * @param conf
   *          to be used to connect to HBase
   * @param processRecords
   *          Set the list of ProcessRecord to PROCESSED.
   * @throws IOException
   */
  private void updateProcessRecords(Configuration conf,
      List<ProcessRecord> processRecords) throws IOException {
    ProcessRecordService processRecordService = new ProcessRecordService(conf);
    IOException caught = null;
    try {

      for (ProcessRecord processRecord : processRecords) {
        // Even if we get an exception, still try to set the other records
        try {
          processRecordService.setProcessState(processRecord, PROCESSED);
        } catch (IOException ioe) {
          caught = ioe;
        }
      }

    } finally {
      try {
        processRecordService.close();
      } catch (IOException ioe) {
        if (caught == null) {
          caught = ioe;
        }
      }
      if (caught != null) {
        throw caught;
      }
    }
  }

  /**
   * @param conf
   *          used to connect to HBAse
   * @param cluster
   *          for which we are processing
   * @param reprocess
   *          Reprocess those records that may have been processed already.
   *          Otherwise successfully processed job files are skipped.
   * @param batchSize
   *          the total number of jobs to process in a batch (a MR job scanning
   *          these many records in the raw table).
   * @param minJobId
   *          used to start the scan. If null then there is no min limit on
   *          JobId.
   * @param maxJobId
   *          used to end the scan (inclusive). If null then there is no max
   *          limit on jobId.
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws ExecutionException
   * @throws RowKeyParseException
   */
  private List<JobRunner> getJobRunners(Configuration conf, String cluster,
      boolean reprocess, int batchSize, String minJobId, String maxJobId)
      throws IOException, InterruptedException, ClassNotFoundException,
      RowKeyParseException {
    List<JobRunner> jobRunners = new LinkedList<JobRunner>();

    JobHistoryRawService jobHistoryRawService = new JobHistoryRawService(conf);
    try {

      // Bind all MR jobs together with one runID.
      long now = System.currentTimeMillis();
      conf.setLong(Constants.MR_RUN_CONF_KEY, now);

      List<Scan> scanList = jobHistoryRawService.getHistoryRawTableScans(
          cluster, minJobId, maxJobId, reprocess, batchSize);

      for (Scan scan : scanList) {
        Job job = getProcessingJob(conf, scan, scanList.size());

        JobRunner jobRunner = new JobRunner(job, null);
        jobRunners.add(jobRunner);
      }

    } finally {
      IOException caught = null;
      try {
        jobHistoryRawService.close();
      } catch (IOException ioe) {
        caught = ioe;
      }

      if (caught != null) {
        throw caught;
      }
    }
    return jobRunners;

  }

  /**
   * @param conf
   *          to use to create and run the job
   * @param scan
   *          to be used to scan the raw table.
   * @param totalJobCount
   *          the total number of jobs that need to be run in this batch. Used
   *          in job name.
   * @return The job to be submitted to the cluster.
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private Job getProcessingJob(Configuration conf, Scan scan, int totalJobCount)
      throws IOException {

    Configuration confClone = new Configuration(conf);

    // Turn off speculative execution.
    // Note: must be BEFORE the job construction with the new mapreduce API.
    confClone.setBoolean("mapred.map.tasks.speculative.execution", false);

    // Set up job
    Job job = new Job(confClone, getJobName(totalJobCount));

    // This is a map-only class, skip reduce step
    job.setNumReduceTasks(0);
    job.setJarByClass(JobFileProcessor.class);
    job.setOutputFormatClass(MultiTableOutputFormat.class);

    TableMapReduceUtil.initTableMapperJob(Constants.HISTORY_RAW_TABLE, scan,
        JobFileTableMapper.class, JobFileTableMapper.getOutputKeyClass(),
        JobFileTableMapper.getOutputValueClass(), job);

    return job;
  }

  /**
   * @param totalJobCount
   *          how many jobs there will be in total. Used as indicator in the
   *          name how far along this job is.
   * @return the name to use for each consecutive Hadoop job to launch.
   */
  private synchronized String getJobName(int totalJobCount) {
    String jobName = NAME + " [" + startTimestamp + " "
        + jobCounter.incrementAndGet() + "/" + totalJobCount + "]";
    return jobName;
  }

  /**
   * DoIt.
   * @param args the arguments to do it with
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new JobFileProcessor(), args);
  }

}
