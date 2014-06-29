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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.twitter.hraven.util.BatchUtil;

/**
 * Command line tool to take a directory and split all the job confs into a
 * hierarchical directory structure in HDFS by date (of the file). Can load from
 * HDFS history done directory, or from a local filesystem (identified as
 * file://)
 * <p>
 * A limit can be specified how many files should be left in the source
 * directory. That is handy for continuously running on the
 * /hadoop/mapred/history/done directory to partition all files, but to keep the
 * growth of the done directory in check.
 */
public class JobFilePartitioner extends Configured implements Tool {

  final static String NAME = JobFilePartitioner.class.getSimpleName();
  private static Log LOG = LogFactory.getLog(JobFilePartitioner.class);

  // Simple format look like this: yyyy-MM-dd HH:mm
  public static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat(
      "yyyy");
  public static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("MM");
  public static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("dd");

  // Initialize to use UTC
  static {
    TimeZone utc = TimeZone.getTimeZone("UTC");
    YEAR_FORMAT.setTimeZone(utc);
    MONTH_FORMAT.setTimeZone(utc);
    DAY_FORMAT.setTimeZone(utc);
  }

  /**
   * To be used for this job.
   */
  Configuration myConf;

  /**
   * Input directory (presumed in HDFS, unless prefixed with file://
   */
  String input;

  /**
   * Whether files that are already in the target directory should be skipped.
   */
  boolean skipExisting = true;

  /**
   * Whether files should be moved rather than copied. Can be used with HDFS
   * input paths only.
   */
  boolean moveFiles = true;

  /**
   * The maximum number of files to retain in the input directory after
   * processing.
   */
  int maXretention = Integer.MAX_VALUE;

  /**
   * Used to read files from (if input is HDFS) and to write output to.
   */
  FileSystem hdfs;

  /**
   * Location in HDFS where to write the output to. Under this directory a
   * year/month/day directory structure will be created.
   */
  Path outputPath;

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
    Option o = new Option("i", "input", true,
        "input directory as hdfs path, or local as file://");
    o.setArgName("input-path");
    o.setRequired(true);
    options.addOption(o);

    // Input
    o = new Option("o", "output", true, "output directory");
    o.setArgName("input-path");
    o.setRequired(true);
    options.addOption(o);

    // Whether to skip existing files or not.
    o = new Option("s", "skipExisting", false,
        "skip existing files. Cannot be used together with m for move.");
    o.setRequired(false);
    options.addOption(o);

    // Maximum number of files to retain in the specified input directory.
    o = new Option(
        "x",
        "maXretention",
        true,
        "The maximum number of the most recent files to retain in the input directory after processing."
            + " Can be used by HDFS input paths only. Mutually exclusive with s (move),"
            + " but can be used in combination with s (skipExisting)");
    o.setRequired(false);
    options.addOption(o);

    // Whether files need to be moved
    o = new Option("m", "move", false, "move all files rather than copying."
        + "Delete source if target already exists."
        + " Can be used with HDFS input paths only. "
        + " Mutually exlusive with s (skipExisting)");
    o.setRequired(false);
    options.addOption(o);

    // Debugging
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    o.setRequired(false);
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

    return commandLine;
  }

  /*
   * Do the actual work.
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {

    myConf = getConf();

    // Presume this is all HDFS paths, even when access as file://
    hdfs = FileSystem.get(myConf);

    // Grab input args and allow for -Dxyz style arguments
    String[] otherArgs = new GenericOptionsParser(myConf, args)
        .getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    // Grab the input path argument
    input = commandLine.getOptionValue("i");
    LOG.info("input=" + input);

    // Grab the input path argument
    String output = commandLine.getOptionValue("o");
    LOG.info("output=" + output);

    skipExisting = commandLine.hasOption("s");
    LOG.info("skipExisting=" + skipExisting);

    moveFiles = commandLine.hasOption("m");
    LOG.info("moveFiles=" + moveFiles);

    if (skipExisting && moveFiles) {
      throw new IllegalArgumentException(
          "Cannot use both options skipExisting and move simultaneously.");
    }

    if (commandLine.hasOption("x")) {
      try {
        maXretention = Integer.parseInt(commandLine.getOptionValue("x"));
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "maXretention option -x is is not a valid number: "
                + commandLine.getOptionValue("x"), nfe);
      }
      // Additional check
      if (maXretention < 0) {
        throw new IllegalArgumentException(
            "Cannot retain less than 0 files. Specified maXretention option -x is: "
                + commandLine.getOptionValue("x"));
      }
      LOG.info("maXretention=" + maXretention);
      if (moveFiles) {
        throw new IllegalArgumentException(
            "Cannot use both options maXretention and move simultaneously.");
      }
    } else {
      maXretention = Integer.MAX_VALUE;
    }

    outputPath = new Path(output);
    FileStatus outputFileStatus = hdfs.getFileStatus(outputPath);

    if (!outputFileStatus.isDir()) {
      throw new IOException("Output is not a directory"
          + outputFileStatus.getPath().getName());
    }

    Path inputPath = new Path(input);
    URI inputURI = inputPath.toUri();
    String inputScheme = inputURI.getScheme();

    LOG.info("input scheme is: " + inputScheme);

    // If input directory is HDFS, then process as such. Assume not scheme is
    // HDFS
    if ((inputScheme == null)
        || (hdfs.getUri().getScheme().equals(inputScheme))) {
      processHDFSSources(inputPath);
    } else if (inputScheme.equals("file")) {
      if (moveFiles) {
        throw new IllegalArgumentException(
            "Cannot move files that are not already in hdfs. Input is not HDFS: "
                + input);
      }
      processPlainFileSources(inputURI);
    } else {
      throw new IllegalArgumentException(
          "Cannot process files from this URI scheme: " + inputScheme);
    }

    Statistics statistics = FileSystem.getStatistics(outputPath.toUri()
        .getScheme(), hdfs.getClass());
    if (statistics != null) {
      LOG.info("HDFS bytes read: " + statistics.getBytesRead());
      LOG.info("HDFS bytes written: " + statistics.getBytesWritten());
      LOG.info("HDFS read ops: " + statistics.getReadOps());
      System.out
          .println("HDFS large read ops: " + statistics.getLargeReadOps());
      LOG.info("HDFS write ops: " + statistics.getWriteOps());
    }

    return 0;
  }

  /**
   * Process HDFS source directory.
   * 
   * @param hdfs
   * @param outputPath
   * @param inputPath
   * @throws IOException
   */
  /**
   * @param inputPath
   * @throws IOException
   */
  /**
   * @param inputPath
   * @throws IOException
   */
  private void processHDFSSources(Path inputPath) throws IOException {
    // Try to get the fileStatus only if we're reasonably confident that this
    // is an HDFS path.s
    FileStatus inputFileStatus = hdfs.getFileStatus(inputPath);

    // Check if input is a directory
    if (!inputFileStatus.isDir()) {
      throw new IOException("Input is not a directory in HDFS: " + input);
    }

    // Accept only jobFiles and only those that fall in the desired range of
    // modification time.
    JobFileModifiedRangeSubstringPathFilter jobFileModifiedRangePathFilter = new JobFileModifiedRangeSubstringPathFilter(
        myConf, 0L);

    ContentSummary contentSummary = hdfs.getContentSummary(inputPath);
    LOG.info("Listing / filtering (" + contentSummary.getFileCount()
        + ") files in: " + inputPath);

    // get the files in the done folder,
    // need to traverse dirs under done recursively for versions
    // that include MAPREDUCE-323: on/after hadoop 0.20.203.0
    // on/after cdh3u5
    FileStatus[] jobFileStatusses = FileLister.listFiles(true, hdfs, inputPath,
        jobFileModifiedRangePathFilter);

    LOG.info("Sorting " + jobFileStatusses.length + " job files.");

    Arrays.sort(jobFileStatusses, new FileStatusModificationComparator());

    int processedCount = 0;
    try {

      for (int i = 0; i < jobFileStatusses.length; i++) {
        FileStatus jobFileStatus = jobFileStatusses[i];

        boolean retain = BatchUtil.shouldRetain(i, maXretention, jobFileStatusses.length);
        processHDFSSource(hdfs, jobFileStatus, outputPath, myConf,
            skipExisting, retain);
        processedCount++;
        // Print something each 1k files to show progress.
        if ((i % 1000) == 0) {
          LOG.info("Processed " + i + " files.");
        }

      }

    } finally {
      LOG.info("Processed " + processedCount + " files.");
    }
  }

 

  /**
   * Input is a regular directory (non-hdfs). Process files accordingly.
   * 
   * @param inputURI
   * @throws IOException
   */
  private void processPlainFileSources(URI inputURI) throws IOException {
    LOG.info("Scheme specific part is: " + inputURI.getSchemeSpecificPart());

    File inputFile = new File(inputURI.getSchemeSpecificPart());

    // Check if input is a directory
    if (!inputFile.isDirectory()) {
      throw new IOException("Input is not a regular directory: " + input);
    }

    File[] files = inputFile.listFiles();
    int processedCount = 0;
    try {
      for (File f : files) {
        // Process only files, not (sub)directories.
        if (f.isFile()) {
          processPlainFile(hdfs, f, outputPath, skipExisting);
          processedCount++;
          // Print something each 100 files to show progress.
          if ((processedCount % 1000) == 0) {
            LOG.info("Processed " + processedCount + " files.");
          }
        }
      }
    } finally {
      LOG.info("Processed " + processedCount + " files.");
    }
  }

  /**
   * @param hdfs
   *          FileSystem handle
   * @param outputPath
   *          base directory where files to be written to
   * @param fileModTime
   *          of the file that needs to be moved/copied to hdfs
   * @return the existing path in HDFS to write to the file to. Will be created
   *         if it does not exist.
   * @throws IOException
   *           if the year/month/day directory with cannot be created in
   *           outputPath.
   */
  private Path getTargetDirectory(FileSystem hdfs, Path outputPath,
      long fileModTime) throws IOException {
    String year = YEAR_FORMAT.format(new Date(fileModTime));
    String month = MONTH_FORMAT.format(new Date(fileModTime));
    String day = DAY_FORMAT.format(new Date(fileModTime));

    Path yearDir = new Path(outputPath, year);
    Path monthDir = new Path(yearDir, month);
    Path dayDir = new Path(monthDir, day);

    // Check if the directory already exists, if not, then insert a record into
    // HBase for it.
    if (!hdfs.exists(dayDir)) {
      if (hdfs.mkdirs(dayDir)) {
        LOG.info("Created: " + dayDir.toString());
      } else {
        throw new IOException("Unable to create target directory with date: "
            + dayDir.getName());
      }
    }
    return dayDir;
  }

  /**
   * @param hdfs
   *          FileSystem handle
   * @param f
   *          file to copy to HDFS
   * @param outputPath
   * @param skipExisting
   *          skip if the file already exist in the target. File will be
   *          overwritten if already there and this argument is false.
   * @throws IOException
   *           if target directory cannot be created or file cannot be copied to
   *           target directory.
   */
  private void processPlainFile(FileSystem hdfs, File f, Path outputPath,
      boolean skipExisting) throws IOException {
    long fileModTime = f.lastModified();
    Path targetDir = getTargetDirectory(hdfs, outputPath, fileModTime);

    boolean doCopy = true;
    Path sourceFile = new Path(f.getPath());
    if (skipExisting) {
      Path target = new Path(targetDir, sourceFile.getName());
      if (hdfs.exists(target)) {
        doCopy = false;
      }
    }
    if (doCopy) {
      hdfs.copyFromLocalFile(sourceFile, targetDir);
    }

  }

  /**
   * @param hdfs
   *          FileSystem handle
   * @param f
   *          file to process
   * @param outputPath
   * @param conf
   *          configuration to use for copying.
   * @param skipExisting
   *          skip if the file already exist in the target. File will be
   *          overwritten if already there and this argument is false.
   * @retain whether this file should be retained
   * 
   * @throws IOException
   */
  private void processHDFSSource(FileSystem hdfs, FileStatus f,
      Path outputPath, Configuration conf, boolean skipExisting, boolean retain)
      throws IOException {

    long fileModTime = f.getModificationTime();
    Path targetDir = getTargetDirectory(hdfs, outputPath, fileModTime);

    boolean targetExists = false;
    Path target = new Path(targetDir, f.getPath().getName());
    targetExists = hdfs.exists(target);

    if (moveFiles || !retain) {
      if (targetExists) {
        hdfs.delete(f.getPath(), false);
      } else {
        hdfs.rename(f.getPath(), targetDir);
      }
    } else {
      if (targetExists && skipExisting) {
        // Do nothing, target is already there and we're instructed to skip
        // existing records.
      } else {
        copy(hdfs, f, conf, targetDir);
      }
    }
  }

  /**
   * @param hdfs FileSystem handle
   * @param f to copy
   * @param conf configuration to use for copying.
   * @param targetDir directory to copy said file to.
   * @throws IOException
   */
  private void copy(FileSystem hdfs, FileStatus f, Configuration conf,
      Path targetDir) throws IOException {
    long startNanos = System.nanoTime();
    FileUtil.copy(hdfs, f.getPath(), hdfs, targetDir, false, true, conf);
    long estimatedTimeNanos = System.nanoTime() - startNanos;
    // Nanos are 10^-9, millis 10^-3
    long durationMillis = estimatedTimeNanos / 1000000;
    if (durationMillis > 3000) {
      String msg = "It took " + durationMillis / 1000 + " seconds to copy "
          + f.getPath().getName() + " of " + f.getLen() + " bytes.";
      LOG.warn(msg);
    }
  }

  /**
   * DoIt.
   * 
   * @param args
   *          the arguments to do it with
   */
  public static void main(String[] args) {
    try {
      ToolRunner.run(new JobFilePartitioner(), args);
    } catch (Exception e) {
      LOG.error("Problem running: " + NAME, e);
    }
  }

}
