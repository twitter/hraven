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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.etl.ProcessRecordService;
import com.twitter.hraven.util.BatchUtil;
import com.twitter.hraven.etl.FileLister;
import com.twitter.hraven.etl.JobFileModifiedRangePathFilter;

/**
 * Command line tool that can be run on a periodic basis (like daily, hourly, or
 * every 15 minutes or 1/2 hour). Each run is recorded by inserting a new
 * {@link ProcessRecord} in {@link ProcessState#CREATED} state. When the total
 * processing completes successfully, then the record state will be updated to
 * {@link ProcessState#PREPROCESSED} to indicate that this batch has been
 * successfully updated. The run start time will be recorded in as
 * {@link ProcessRecord#getMaxModificationTimeMillis()} so it can be used as the
 * starting mark for the next run if the previous run is successful.
 * 
 * Given the sloooow copying of 100k little files in Hadoop (pull from HDFS,
 * push back in) we need to run this as multiple mappers. - Pull the last
 * process date from HBase. - Insert a new record into HBase with the last date
 * as the start and the current date as the end. - Create a map-reduce job that
 * reads whole files, combine, and set a min to have multiple maps. - Then copy
 * files and emit the smallest job_id as a key and a timestamp as a value - Then
 * have a combiner that combines keys/values - then pick up the result from the
 * smallest number - Then update record in HBase with the processing date to
 * mark that processing finished (or not).
 * 
 */
public class JobFilePreprocessor extends Configured implements Tool {

  public final static String NAME = JobFilePreprocessor.class.getSimpleName();
  private static Log LOG = LogFactory.getLog(JobFilePreprocessor.class);

  /**
   * Maximum number of files to process in one batch.
   */
  private final static int DEFAULT_BATCH_SIZE = 1000;

  /**
   * Maximum size of file that be loaded into raw table : 500 MB
   */
  private final static long DEFAULT_RAW_FILE_SIZE_LIMIT = 524288000;

  /**
   * Name of the job conf property used to pass the output directory to the
   * mappers.
   */
  public final static String JOB_RECORD_KEY_LABEL = NAME + ".job.record.key";

  /**
   * Default constructor.
   */
  public JobFilePreprocessor() {
  }

  /**
   * Used for injecting confs while unit testing
   * 
   * @param conf
   */
  public JobFilePreprocessor(Configuration conf) {
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

    // Cluster
    Option o = new Option("c", "cluster", true,
        "cluster for which jobs are processed");
    o.setArgName("cluster");
    o.setRequired(true);
    options.addOption(o);

    // Input
    o = new Option("o", "output", true,
        "output directory in hdfs. This is where the index files are written.");
    o.setArgName("output-path");
    o.setRequired(true);
    options.addOption(o);

    // Input
    o = new Option(
        "i",
        "input",
        true,
        "input directory in hdfs. Default is mapred.job.tracker.history.completed.location.");
    o.setArgName("input-path");
    o.setRequired(false);
    options.addOption(o);

    // Batch
    o = new Option("b", "batchSize", true,
        "The number of files to process in one batch. Default "
            + DEFAULT_BATCH_SIZE);
    o.setArgName("batch-size");
    o.setRequired(false);
    options.addOption(o);

    // raw file size limit
    o = new Option("s", "rawFileSize", true,
        "The max size of file that can be loaded into raw table. Default "
            + DEFAULT_RAW_FILE_SIZE_LIMIT);
    o.setArgName("rawfile-size");
    o.setRequired(false);
    options.addOption(o);

    // Force
    o = new Option(
        "f",
        "forceAllFiles",
        false,
        "Force all files in a directory to be processed, no matter the previous processingRecord. Default: false. Usefull for batch loads.");
    o.setRequired(false);
    options.addOption(o);

    // Accept a minModificationTimeMillis. Don't process files before this time.
    o = new Option("m", "minModificationTimeMillis", true,
            "The minimum modification time of the file to be processed");
    o.setArgName("minModificationTimeMillis");
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
   * Do the actual work.
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {

    // When we started processing. This is also the upper limit of files we
    // accept, next run will pick up the new incoming files.
    long processingStartMillis = System.currentTimeMillis();

    Configuration hbaseConf = HBaseConfiguration.create(getConf());

    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs = new GenericOptionsParser(hbaseConf, args)
        .getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    // Output should be an hdfs path.
    FileSystem hdfs = FileSystem.get(hbaseConf);

    // Grab the input path argument
    String output = commandLine.getOptionValue("o");
    LOG.info(" output=" + output);
    Path outputPath = new Path(output);
    FileStatus outputFileStatus = hdfs.getFileStatus(outputPath);

    if (!outputFileStatus.isDir()) {
      throw new IOException("Output is not a directory"
          + outputFileStatus.getPath().getName());
    }

    // Grab the input path argument
    String input;
    if (commandLine.hasOption("i")) {
      input = commandLine.getOptionValue("i");
    } else {
      input = hbaseConf.get("mapred.job.tracker.history.completed.location");
    }
    LOG.info("input=" + input);

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

    boolean forceAllFiles = commandLine.hasOption("f");
    LOG.info("forceAllFiles: " + forceAllFiles);

    Path inputPath = new Path(input);
    FileStatus inputFileStatus = hdfs.getFileStatus(inputPath);

    if (!inputFileStatus.isDir()) {
      throw new IOException("Input is not a directory"
          + inputFileStatus.getPath().getName());
    }

    // Grab the cluster argument
    String cluster = commandLine.getOptionValue("c");
    LOG.info("cluster=" + cluster);

    /**
     * Grab the size of huge files to be moved argument
     * hbase cell can't store files bigger than
     * maxFileSize, hence no need to consider them for rawloading
     * Reference:
     * {@link https://github.com/twitter/hraven/issues/59}
     */
    String maxFileSizeStr = commandLine.getOptionValue("s");
    LOG.info("maxFileSize=" + maxFileSizeStr);
    long maxFileSize = DEFAULT_RAW_FILE_SIZE_LIMIT;
    try {
      maxFileSize = Long.parseLong(maxFileSizeStr);
    } catch (NumberFormatException nfe) {
      throw new ProcessingException("Caught NumberFormatException during conversion "
            + " of maxFileSize to long", nfe);
    }
    
    // Grab the minModificationTimeMillis argument
    long minModificationTimeMillis = 0;
    
    if (commandLine.getOptionValue("m") != null) {
      try {
        minModificationTimeMillis = Long.parseLong(commandLine.getOptionValue("m"));
        LOG.info("Using specified start time for filtering history files: " + minModificationTimeMillis);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "minModificationTimeMillis has to be an epoch time (long). Can't be: "
                + commandLine.getOptionValue("m"), nfe);
      }  
    }
    
    LOG.info("minModificationTimeMillis: " + minModificationTimeMillis);

    ProcessRecordService processRecordService = new ProcessRecordService(
        hbaseConf);

    boolean success = true;
    try {

      // Figure out where we last left off (if anywhere at all)
      ProcessRecord lastProcessRecord = null;

      if (!forceAllFiles) {
        lastProcessRecord = processRecordService
            .getLastSuccessfulProcessRecord(cluster);
      } else {
        //discard minModificationTimeMillis arguemnt given if all files
        //are to be forced.
        minModificationTimeMillis = 0;
      }

      // Start of this time period is the end of the last period.
      if (lastProcessRecord != null) {
        LOG.info("lastProcessRecord time: " + lastProcessRecord.getMaxModificationTimeMillis());
        // Choose the maximum of the two.
        if (minModificationTimeMillis < lastProcessRecord
            .getMaxModificationTimeMillis()) {
          minModificationTimeMillis = lastProcessRecord
              .getMaxModificationTimeMillis();
          LOG.info("lastProcessRecord is greater than minModificationTimeMillis. Using that as minimum time: "
              + minModificationTimeMillis);
        }
      }

      // Do a sanity check. The end time of the last scan better not be later
      // than when we started processing.
      if (minModificationTimeMillis > processingStartMillis) {
        throw new RuntimeException(
            "Job start time is lesser than the minimum modification time of files to read. Failing."
                + lastProcessRecord);
      }

      // Accept only jobFiles and only those that fall in the desired range of
      // modification time.
      JobFileModifiedRangePathFilter jobFileModifiedRangePathFilter = new JobFileModifiedRangePathFilter(
          hbaseConf, minModificationTimeMillis);

      String timestamp = Constants.TIMESTAMP_FORMAT.format(new Date(
          minModificationTimeMillis));

      ContentSummary contentSummary = hdfs.getContentSummary(inputPath);
      LOG.info("Listing / filtering ("
          + contentSummary.getFileCount() + ") files in: " + inputPath
          + " that are modified since " + timestamp);

      // get the files in the done folder,
      // need to traverse dirs under done recursively for versions
      // that include MAPREDUCE-323: on/after hadoop 0.20.203.0
      // on/after cdh3u5
      FileStatus[] jobFileStatusses = FileLister.getListFilesToProcess(maxFileSize, true,
            hdfs, inputPath, jobFileModifiedRangePathFilter);

      LOG.info("Sorting " + jobFileStatusses.length + " job files.");

      Arrays.sort(jobFileStatusses, new FileStatusModificationComparator());

      // Process these files in batches at a time.
      int batchCount = BatchUtil.getBatchCount(jobFileStatusses.length, batchSize);
      LOG.info("Batch count: " + batchCount);
      for (int b = 0; b < batchCount; b++) {
        processBatch(jobFileStatusses, b, batchSize, processRecordService,
            cluster, outputPath);
      }

    } finally {
      processRecordService.close();
    }

    Statistics statistics = FileSystem.getStatistics(inputPath.toUri()
        .getScheme(), hdfs.getClass());
    if (statistics != null) {
      LOG.info("HDFS bytes read: " + statistics.getBytesRead());
      LOG.info("HDFS bytes written: " + statistics.getBytesWritten());
      LOG.info("HDFS read ops: " + statistics.getReadOps());
      LOG.info("HDFS large read ops: " + statistics.getLargeReadOps());
      LOG.info("HDFS write ops: " + statistics.getWriteOps());
    }

    // Return the status
    return success ? 0 : 1;
  }



  /**
   * @param jobFileStatusses
   *          statusses sorted by modification time.
   * @param batch
   *          which batch needs to be processed (used to calculate offset in
   *          jobFileStatusses.
   * @param batchSize
   *          process up to length items (or less as to not exceed the length of
   *          jobFileStatusses
   * @param processRecordService
   *          to be used to access create ProcessRecords.
   * @throws IOException
   *           when the index file cannot be written or moved, or when the HBase
   *           records cannot be created.
   */
  private void processBatch(FileStatus jobFileStatusses[], int batch,
      int batchSize, ProcessRecordService processRecordService, String cluster,
      Path outputPath) throws IOException {

    int startIndex = batch * batchSize;

    LOG.info("Batch startIndex: " + startIndex + " batchSize: "
        + batchSize);

    // Some protection against over and under runs.
    if ((jobFileStatusses == null) || (startIndex < 0)
        || (startIndex >= jobFileStatusses.length)) {
      return;
    }

    MinMaxJobFileTracker minMaxJobFileTracker = new MinMaxJobFileTracker();

    Path initialProcesFile = processRecordService.getInitialProcessFile(
        cluster, batch);
    Writer processFileWriter = processRecordService
        .createProcessFileWriter(initialProcesFile);

    // Make sure we don't run off the end of the array
    int endIndexExclusive = Math.min((startIndex + batchSize),
        jobFileStatusses.length);
    try {
      for (int i = startIndex; i < endIndexExclusive; i++) {
        FileStatus fileStatus = jobFileStatusses[i];
        JobFile jobFile = minMaxJobFileTracker.track(fileStatus);

        // String jobfileName = fileStatus.getPath().getName();
        // LOG.info(jobfileName + " modified: "
        // + fileStatus.getModificationTime());

        processFileWriter.append(jobFile, fileStatus);
      }

    } finally {
      processFileWriter.close();
    }

    Path processFile = processRecordService.moveProcessFile(initialProcesFile,
        outputPath);

    int processedJobFiles = endIndexExclusive - startIndex;

    ProcessRecord processRecord = new ProcessRecord(cluster,
        ProcessState.PREPROCESSED,
        minMaxJobFileTracker.getMinModificationTimeMillis(),
        minMaxJobFileTracker.getMaxModificationTimeMillis(), processedJobFiles,
        processFile.toString(), minMaxJobFileTracker.getMinJobId(),
        minMaxJobFileTracker.getMaxJobId());

    LOG.info("Creating processRecord: " + processRecord);

    processRecordService.writeJobRecord(processRecord);

  }

  /**
   * DoIt.
   * 
   * @param args
   *          the arguments to do it with
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new JobFilePreprocessor(), args);
  }

}
