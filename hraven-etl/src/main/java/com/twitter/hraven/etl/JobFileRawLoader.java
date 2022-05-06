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
package com.twitter.hraven.etl;

import static com.twitter.hraven.etl.ProcessState.PREPROCESSED;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.twitter.hraven.Constants;
import com.twitter.hraven.mapreduce.JobFileRawLoaderMapper;

/**
 * Used to load the job files from an HDFS directory to an HBase table. This is
 * just the raw loading part of the process. A process table is used to record
 * the lowest job_id encountered during this load.
 *
 */
public class JobFileRawLoader extends Configured implements Tool {

  final static String NAME = JobFileRawLoader.class.getSimpleName();
  private static Log LOG = LogFactory.getLog(JobFileRawLoader.class);

  private final String startTimestamp =
      Constants.TIMESTAMP_FORMAT.format(new Date(System.currentTimeMillis()));

  private final AtomicInteger jobCounter = new AtomicInteger(0);

  /**
   * Used to read files from (if input is HDFS) and to write output to.
   */
  FileSystem hdfs;

  /**
   * Default constructor
   */
  public JobFileRawLoader() {
  }

  /**
   * Used for injecting confs while unit testing
   *
   * @param conf
   */
  public JobFileRawLoader(Configuration conf) {
    super(conf);

  }

  /**
   * Parse command-line arguments.
   *
   * @param args command line arguments passed to program.
   * @return parsed command line.
   * @throws ParseException
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();

    // Cluster
    Option o = new Option("c", "cluster", true,
        "cluster for which jobs are processed");
    o.setArgName("cluster");
    o.setRequired(true);
    options.addOption(o);

    o = new Option("p", "processFileSubstring", true,
        "use only those process records where the process file path contains the provided string. Useful when processing production jobs in parallel to historic loads.");
    o.setArgName("processFileSubstring");
    o.setRequired(false);
    options.addOption(o);

    // Force
    o = new Option("f", "forceReprocess", false,
        "Force all jobs for which a jobFile is loaded to be reprocessed. Optional. Default is false.");
    o.setRequired(false);
    options.addOption(o);

    // Debugging
    options.addOption("d", "debug", false, "switch on DEBUG log level");

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
      Logger log = Logger.getLogger(JobFileRawLoader.class);
      log.setLevel(Level.DEBUG);
    }

    return commandLine;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  public int run(String[] args) throws ParseException, IOException,
      ClassNotFoundException, InterruptedException {

    Configuration hbaseConf = HBaseConfiguration.create(getConf());
    hdfs = FileSystem.get(hbaseConf);

    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs =
        new GenericOptionsParser(hbaseConf, args).getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    String input = null;
    boolean inputSpecified = commandLine.hasOption("i");
    if (inputSpecified) {
      // Grab the input path argument
      input = commandLine.getOptionValue("i");
      LOG.info("input=" + input);
    } else {
      LOG.info("Processing input from HBase ProcessRecords");
    }

    // Grab the cluster argument
    String cluster = commandLine.getOptionValue("c");
    LOG.info("cluster=" + cluster);

    String processFileSubstring = null;
    if (commandLine.hasOption("p")) {
      processFileSubstring = commandLine.getOptionValue("p");
    }
    LOG.info("processFileSubstring=" + processFileSubstring);

    boolean forceReprocess = commandLine.hasOption("f");
    LOG.info("forceReprocess: " + forceReprocess);

    // hbase.client.keyvalue.maxsize somehow defaults to 10 MB and we have
    // history files exceeding that. Disable limit.
    hbaseConf.setInt("hbase.client.keyvalue.maxsize", 0);

    // Shove this into the jobConf so that we can get it out on the task side.
    hbaseConf.setStrings(Constants.CLUSTER_JOB_CONF_KEY, cluster);

    boolean success = true;
    Connection hbaseConnection = null;
    try {
      hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
      success = processRecordsFromHBase(hbaseConf, hbaseConnection, cluster,
          processFileSubstring, forceReprocess);
    } finally {
      if (hbaseConnection == null) {
        success = false;
      } else {
        hbaseConnection.close();
      }
    }

    // Return the status
    return success ? 0 : 1;
  }

  /**
   * @param myHBaseConf used to contact HBase and to run jobs against. Should be
   *          an HBase configuration.
   * @param hbaseConnection
   * @param cluster for which to process records.
   * @param processFileSubstring return rows where the process file path
   *          contains this string. If <code>null</code> or empty string, then
   *          no filtering is applied.
   * @param forceReprocess whether all jobs for which a file is loaded needs to
   *          be reprocessed.
   * @return whether all job files for all processRecords were properly
   *         processed.
   * @throws IOException
   * @throws ClassNotFoundException when problems occur setting up the job.
   * @throws InterruptedException
   */
  private boolean processRecordsFromHBase(Configuration myHBaseConf,
      Connection hbaseConnection, String cluster, String processFileSubstring,
      boolean forceReprocess)
      throws IOException, InterruptedException, ClassNotFoundException {

    int failures = 0;

    ProcessRecordService processRecordService =
        new ProcessRecordService(myHBaseConf, hbaseConnection);
    // Grab all records.
    List<ProcessRecord> processRecords = processRecordService.getProcessRecords(
        cluster, PREPROCESSED, Integer.MAX_VALUE, processFileSubstring);

    LOG.info("ProcessRecords for " + cluster + ": " + processRecords.size());

    // Bind all MR jobs together with one runID.
    long now = System.currentTimeMillis();
    myHBaseConf.setLong(Constants.MR_RUN_CONF_KEY, now);

    myHBaseConf.setBoolean(Constants.FORCE_REPROCESS_CONF_KEY, forceReprocess);

    // Iterate over 0 based list in reverse order
    for (int j = processRecords.size() - 1; j >= 0; j--) {
      ProcessRecord processRecord = processRecords.get(j);

      LOG.info("Processing " + processRecord);

      boolean success = runRawLoaderJob(myHBaseConf,
          processRecord.getProcessFile(), processRecords.size());
      // Bail out on first failure.
      if (success) {
        processRecordService.setProcessState(processRecord,
            ProcessState.LOADED);
      } else {
        failures++;
      }

    }

    return (failures == 0);
  }

  /**
   * @param conf to use to create and run the job. Should be an HBase
   *          configuration.
   * @param input path to the processFile * @param totalJobCount the total
   *          number of jobs that need to be run in this batch. Used in job
   *          name.
   * @return whether all job confs were loaded properly.
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private boolean runRawLoaderJob(Configuration myHBaseConf, String input,
      int totalJobCount)
      throws IOException, InterruptedException, ClassNotFoundException {
    boolean success;

    // Turn off speculative execution.
    // Note: must be BEFORE the job construction with the new mapreduce API.
    myHBaseConf.setBoolean("mapred.map.tasks.speculative.execution", false);

    // Set tmpjars for hadoop to be able to find hraven-core and other required libs
    HadoopUtil.setTmpJars(Constants.HRAVEN_HDFS_LIB_PATH_CONF, myHBaseConf);
    
    // Set up job
    Job job = new Job(myHBaseConf, getJobName(totalJobCount));
    job.setJarByClass(JobFileRawLoader.class);

    Path inputPath = new Path(input);

    if (hdfs.exists(inputPath)) {

      // Set input
      job.setInputFormatClass(SequenceFileInputFormat.class);
      SequenceFileInputFormat.setInputPaths(job, inputPath);

      job.setMapperClass(JobFileRawLoaderMapper.class);

      // Set the output format to push data into HBase.
      job.setOutputFormatClass(TableOutputFormat.class);
      TableMapReduceUtil.initTableReducerJob(Constants.HISTORY_RAW_TABLE, null,
          job);

      job.setOutputKeyClass(JobFileRawLoaderMapper.getOutputKeyClass());
      job.setOutputValueClass(JobFileRawLoaderMapper.getOutputValueClass());

      // This is a map-only class, skip reduce step
      job.setNumReduceTasks(0);

      // Run the job
      success = job.waitForCompletion(true);

      if (success) {
        success = hdfs.delete(inputPath, false);
      }

    } else {
      System.err.println("Unable to find processFile: " + inputPath);
      success = false;
    }
    return success;
  }

  /**
   * @param totalJobCount how many jobs there will be in total. Used as
   *          indicator in the name how far along this job is.
   * @return the name to use for each consecutive Hadoop job to launch.
   */
  private synchronized String getJobName(int totalJobCount) {
    String jobName = NAME + " [" + startTimestamp + " "
        + jobCounter.incrementAndGet() + "/" + totalJobCount + "]";
    return jobName;
  }

  /**
   * DoIt.
   *
   * @param args the arguments to do it with
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new JobFileRawLoader(), args);
  }

}
