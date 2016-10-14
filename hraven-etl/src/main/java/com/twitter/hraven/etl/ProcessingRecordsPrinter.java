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

import java.io.IOException;
import java.util.List;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Small utility used to print all processing records given a cluster.
 *
 */
public class ProcessingRecordsPrinter extends Configured implements Tool {

  final static String NAME = ProcessingRecordsPrinter.class.getSimpleName();
  private static Log LOG = LogFactory.getLog(ProcessingRecordsPrinter.class);

  /**
   * Default constructor.
   */
  public ProcessingRecordsPrinter() {
  }

  /**
   * Used for injecting confs while unit testing
   *
   * @param conf
   */
  public ProcessingRecordsPrinter(Configuration conf) {
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

    // Input
    Option o = new Option("m", "maxCount", true,
        "maximum number of records to be returned");
    o.setArgName("maxCount");
    o.setRequired(false);
    options.addOption(o);

    o = new Option("c", "cluster", true,
        "cluster for which jobs are processed");
    o.setArgName("cluster");
    o.setRequired(true);
    options.addOption(o);

    o = new Option("p", "processFileSubstring", true,
        "use only those process records where the process file path contains the provided string.");
    o.setArgName("processFileSubstring");
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
      Logger log = Logger.getLogger(ProcessingRecordsPrinter.class);
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
    String[] otherArgs =
        new GenericOptionsParser(hbaseConf, args).getRemainingArgs();

    // Grab the arguments we're looking for.
    CommandLine commandLine = parseArgs(otherArgs);

    // Grab the cluster argument
    String cluster = commandLine.getOptionValue("c");
    LOG.info("cluster=" + cluster);

    // Grab the cluster argument

    String processFileSubstring = null;
    if (commandLine.hasOption("p")) {
      processFileSubstring = commandLine.getOptionValue("p");
    }
    LOG.info("processFileSubstring=" + processFileSubstring);

    // Default to no max
    Integer maxCount = Integer.MAX_VALUE;
    if (commandLine.hasOption("m")) {
      try {
        maxCount = Integer.parseInt(commandLine.getOptionValue("m"));
      } catch (NumberFormatException nfe) {
        System.err.println("Error: " + NAME + " maxCount is not an integer: "
            + commandLine.getOptionValue("m"));
      }
    }

    boolean success = true;
    Connection hbaseConnection = null;
    try {
      hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
      success = printProcessRecordsFromHBase(hbaseConf, hbaseConnection,
          cluster, maxCount, processFileSubstring);
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
   * @param conf used to contact HBase and to run jobs against
   * @param hbaseConnection
   * @param cluster for which to process records.
   * @param processFileSubstring return rows where the process file path
   *          contains this string. If <code>null</code> or empty string, then
   *          no filtering is applied.
   * @return whether all job files for all processRecords were properly Printed.
   * @throws IOException
   */
  private boolean printProcessRecordsFromHBase(Configuration conf,
      Connection hbaseConnection, String cluster, int maxCount,
      String processFileSubstring) throws IOException {
    ProcessRecordService processRecordService =
        new ProcessRecordService(conf, hbaseConnection);
    List<ProcessRecord> processRecords = processRecordService
        .getProcessRecords(cluster, maxCount, processFileSubstring);

    int jobFileCount = 0;

    System.out.println(
        "ProcessRecords for " + cluster + ": " + processRecords.size());

    // Iterate over 0 based list in reverse order
    for (int j = processRecords.size() - 1; j >= 0; j--) {
      ProcessRecord processRecord = processRecords.get(j);

      // Print the whole thing.
      System.out.println(processRecord);
      jobFileCount += processRecord.getProcessedJobFiles();
    }
    System.out.println("Printed " + processRecords.size()
        + " records with a total of " + jobFileCount + " files.");

    return true;
  }

  /**
   * DoIt.
   *
   * @param args the arguments to do it with
   */
  public static void main(String[] args) {
    try {
      ToolRunner.run(new ProcessingRecordsPrinter(), args);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
