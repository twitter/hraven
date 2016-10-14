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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.etl.JobFile;

/**
 * Used to read records for the processFile (referring to a JobFile). Reads said
 * file into the RAW HBase table.
 */
public class JobFileRawLoaderMapper extends
    Mapper<JobFile, FileStatus, ImmutableBytesWritable, Put> {

  private static final ImmutableBytesWritable EMPTY = new ImmutableBytesWritable();
  private static Log LOG = LogFactory.getLog(JobFileRawLoaderMapper.class);

  private long keyCount = 0;

  private boolean forceReprocess = false;

  /**
   * Used to read the files from.
   */
  private FileSystem hdfs;

  /**
   * Job configuration for this job.
   */
  private Configuration myConf;

  /**
   * Service for storing and retrieving job history and conf blobs.
   */
  private JobHistoryRawService rawService = null;

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
  protected void setup(Context context) throws java.io.IOException,
      InterruptedException {

    myConf = context.getConfiguration();
    hdfs = FileSystem.get(myConf);
    rawService = new JobHistoryRawService(myConf);

    forceReprocess = myConf.getBoolean(Constants.FORCE_REPROCESS_CONF_KEY,
        false);
    LOG.info("forceReprocess=" + forceReprocess);

    keyCount = 0;
  }

  @Override
  protected void map(JobFile jobFile, FileStatus fileStatus, Context context)
      throws IOException, InterruptedException {

    boolean exists = hdfs.exists(fileStatus.getPath());

    if (exists) {
      /**
       * To collect puts to be passed to the mapper.
       */
      List<Put> puts = new LinkedList<Put>();

      // Determine if we need to process this file.
      if (jobFile.isJobConfFile()) {
        keyCount++;
        byte[] rowKey = getRowKeyBytes(jobFile);
        addFileNamePut(puts, rowKey, Constants.JOBCONF_FILENAME_COL_BYTES,
            jobFile.getFilename());
        addRawPut(puts, rowKey, Constants.JOBCONF_COL_BYTES,
            Constants.JOBCONF_LAST_MODIFIED_COL_BYTES, fileStatus);
        if (forceReprocess) {
          // Indicate that we processed the RAW was reloaded so that we can be
          // picked up in the new process scan.
          Put successPut = rawService.getJobProcessedSuccessPut(rowKey, false);
          puts.add(successPut);
        }
        LOG.info("Loaded conf file (" + keyCount + ") size: "
            + fileStatus.getLen() + " = " + jobFile.getFilename());
      } else if (jobFile.isJobHistoryFile()) {
        keyCount++;
        byte[] rowKey = getRowKeyBytes(jobFile);
        // Add filename to be used to re-create JobHistory URL later
        addFileNamePut(puts, rowKey, Constants.JOBHISTORY_FILENAME_COL_BYTES,
            jobFile.getFilename());
        addRawPut(puts, rowKey, Constants.JOBHISTORY_COL_BYTES,
            Constants.JOBHISTORY_LAST_MODIFIED_COL_BYTES, fileStatus);
        if (forceReprocess) {
          // Indicate that we processed the RAW was reloaded so that we can be
          // picked up in the new process scan.
          Put successPut = rawService.getJobProcessedSuccessPut(rowKey, false);
          puts.add(successPut);
        }
        LOG.info("Loaded history file (" + keyCount + ") size: "
            + fileStatus.getLen() + " = " + jobFile.getFilename());
      } else {
        System.out.println("Skipping Key: " + jobFile.getFilename());
      }

      for (Put put : puts) {
        // Key is ignored, value is a Put
        context.write(EMPTY, put);
      }
    } else {
      // TODO: have better error handling.
      System.err.println("Unable to find file: " + fileStatus.getPath());
    }

  };

  /**
   * @param jobFile
   * @return the byte representation of the rowkey for the raw table.
   */
  private byte[] getRowKeyBytes(JobFile jobFile) {
    // This is the cluster for which we are processing files.
    String cluster = myConf.get(Constants.CLUSTER_JOB_CONF_KEY);
    return rawService.getRowKey(cluster, jobFile.getJobid());
  }

  /**
   * @param puts
   *          to add puts to
   * @param rowKey
   *          for the raw table
   * @param filenameColumn
   *          which filename this is (could be for the jobConf of jobHistory
   *          file).
   * @param filename
   *          the name of the file.
   */
  private void addFileNamePut(List<Put> puts, byte[] rowKey,
      byte[] filenameColumn, String filename) {
    Put put = new Put(rowKey);
    put.add(Constants.INFO_FAM_BYTES, filenameColumn, Bytes.toBytes(filename));
    puts.add(put);
  }

  /**
   * Call {@link #readJobFile(FileStatus)} and add the raw bytes and the last
   * modified millis to {@code puts}
   * 
   * @param puts
   *          to add puts to.
   * @rowkey to identify the row in the raw table.
   * @param rawColumn
   *          where to add the raw data in
   * @param fileStatus
   *          Referring to the jobFile to load.
   * @throws IOException
   */
  private void addRawPut(List<Put> puts, byte[] rowKey, byte[] rawColumn,
      byte[] lastModificationColumn, FileStatus fileStatus) throws IOException {
    byte[] rawBytes = readJobFile(fileStatus);

    Put raw = new Put(rowKey);

    byte[] rawLastModifiedMillis = Bytes.toBytes(fileStatus
        .getModificationTime());

    raw.add(Constants.RAW_FAM_BYTES, rawColumn, rawBytes);
    raw.add(Constants.INFO_FAM_BYTES, lastModificationColumn,
        rawLastModifiedMillis);
    puts.add(raw);
  }

  /**
   * Get the raw bytes and the last modification millis for this JobFile
   * 
   * @return the contents of the job file.
   * @throws IOException
   *           when bad things happen during reading
   */
  private byte[] readJobFile(FileStatus fileStatus) throws IOException {
    byte[] rawBytes = null;
    FSDataInputStream fsdis = null;
    try {
      long fileLength = fileStatus.getLen();
      int fileLengthInt = (int) fileLength;
      rawBytes = new byte[fileLengthInt];
      fsdis = hdfs.open(fileStatus.getPath());
      IOUtils.readFully(fsdis, rawBytes, 0, fileLengthInt);
    } finally {
      IOUtils.closeStream(fsdis);
    }
    return rawBytes;
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    if (rawService != null) {
      rawService.close();
    }
  }

}
