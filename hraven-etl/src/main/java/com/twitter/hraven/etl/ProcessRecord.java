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

import java.util.Date;

import com.twitter.hraven.Constants;

/**
 * Used to keep track of a JobFile processing run.
 */
public class ProcessRecord {

  /**
   * Used to store this record in HBase.
   */
  private final ProcessRecordKey key;

  /**
   * The cluster on which the jobs ran that we are recording job history for.
   */
  private final String cluster;

  /**
   * Keeps track of the state of the processing of a bunch of job conf and job
   * history files.
   */
  private final ProcessState processState;

  /**
   * The minimum modification time of a file in milliseconds since January 1,
   * 1970 UTC (excluding) encountered in this batch of files.
   */
  private final long minModificationTimeMillis;

  /**
   * The maximum modification time of a file in milliseconds since January 1,
   * 1970 UTC (including) encountered in this batch of files.
   */
  private final long maxModificationTimeMillis;

  /**
   * How many Job files have been processed successfully. For each job there
   * would typically be 2 files: a conf and a history file.
   */
  private final int processedJobFiles;

  /**
   * The file in hdfs where the job file names are stored for this process run.
   */
  private final String processFile;

  /**
   * The minimum job ID in this batch of jobs. Used to efficiently scan the RAW
   * table. Will not be available until {@link #getProcessState()} is in
   * {@link ProcessState#PREPROCESSED} state or later. In other words, could be
   * null when in {@link ProcessState#CREATED} state.
   */
  private final String minJobId;

  /**
   * The maximum job ID in this batch of jobs. Used to efficiently scan the RAW
   * table. Will not be available until {@link #getProcessState()} is in
   * {@link ProcessState#PREPROCESSED} state or later. In other words, could be
   * null when in {@link ProcessState#CREATED} state.
   */
  private final String maxJobId;

  // TODO: Add identifier who last wrote/updated a record.

  /**
   * Representing one batch of JobFiles processed in its initial state.
   * 
   * @param cluster
   *          the cluster for which we are processing job files. Any
   *          {@link Constants#SEP} sub-strings will be stripped out.
   * @param minModificationTimeMillis
   *          The minimum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (excluding).
   * @param maxModificationTimeMillis
   *          The maximum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (including).
   * @param processedJobFiles
   *          How many Job files have been processed successfully. For each job
   *          there would typically be 2 files: a conf and a history file.
   * @param processFile
   *          The file in hdfs where the job file names are stored for this
   *          process run.
   */
  public ProcessRecord(String cluster, long minModificationTimeMillis,
      long maxModificationTimeMillis, int processedJobFiles,
      String processingDirectory) {
    this(cluster, ProcessState.CREATED, minModificationTimeMillis,
        maxModificationTimeMillis, processedJobFiles, processingDirectory,
        null, null);

  }

  /**
   * Representing one batch of JobFiles processed.
   * 
   * @param cluster
   *          the cluster for which we are processing job files. Any
   *          {@link Constants#SEP} sub-strings will be stripped out.
   * @param processState
   *          to indicate what kind of processing has happend on this batch of
   *          Job Conf and History files.
   * @param minModificationTimeMillis
   *          The minimum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (excluding).
   * @param maxModificationTimeMillis
   *          The maximum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (including).
   * @param processedJobFiles
   *          How many Job files have been processed successfully. For each job
   *          there would typically be 2 files: a conf and a history file.
   * @param processFile
   *          The file in hdfs where the job file names are stored for this
   *          process run.
   * @param minJobId
   *          The minimum job ID in this batch of jobs. Used to efficiently scan
   *          the RAW table. Will not be available until
   *          {@link #getProcessState()} is in {@link ProcessState#PREPROCESSED}
   *          state or later. In other words, could be null when in
   *          {@link ProcessState#CREATED} state.
   * @param maxJobId
   *          The maximum job ID in this batch of jobs. Used to efficiently scan
   *          the RAW table. Will not be available until
   *          {@link #getProcessState()} is in {@link ProcessState#PREPROCESSED}
   *          state or later. In other words, could be null when in
   *          {@link ProcessState#CREATED} state.
   */
  public ProcessRecord(String cluster, ProcessState processState,
      long minModificationTimeMillis, long maxModificationTimeMillis,
      int processedJobFiles, String processFile, String minJobId,
      String maxJobId) {

    this.key = new ProcessRecordKey(cluster, maxModificationTimeMillis);
    // Note that we have NOT ripped out the separators here.
    this.cluster = cluster;
    this.processState = processState;
    this.minModificationTimeMillis = minModificationTimeMillis;
    this.maxModificationTimeMillis = maxModificationTimeMillis;
    this.processedJobFiles = processedJobFiles;
    this.processFile = processFile;
    this.minJobId = minJobId;
    this.maxJobId = maxJobId;
  }

  /**
   * @return the key to be used to store this processing record. It is stored so
   *         that records are ordered first by cluster and then with the most
   *         recent record first.
   */
  public ProcessRecordKey getKey() {
    return key;
  }

  /**
   * @return the cluster for which we are processing job files.
   */
  public String getCluster() {
    return cluster;
  }

  /**
   * @return the last processStae of this processRecord.
   */
  public ProcessState getProcessState() {
    return processState;
  }

  /**
   * @return The minimum modification time of a file in milliseconds since
   *         January 1, 1970 UTC (excluding) encountered in this batch of files.
   */
  public long getMinModificationTimeMillis() {
    return minModificationTimeMillis;
  }

  /**
   * @return The maximum modification time of a file in milliseconds since
   *         January 1, 1970 UTC (including) encountered in this batch of
   *         files..
   */
  public long getMaxModificationTimeMillis() {
    return maxModificationTimeMillis;
  }

  /**
   * @return How many Job files have been processed successfully. For each job
   *         there would typically be 2 files: a conf and a history file.
   */
  public int getProcessedJobFiles() {
    return processedJobFiles;
  }

  /**
   * @return The file in hdfs where the job file names are stored for this process run.
   */
  public String getProcessFile() {
    return processFile;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {

    String minTimestamp = Constants.TIMESTAMP_FORMAT.format(new Date(
        minModificationTimeMillis));
    String maxTimestamp = Constants.TIMESTAMP_FORMAT.format(new Date(
        maxModificationTimeMillis));

    String me = ProcessRecord.class.getSimpleName();
    me += "(" + key + ") ";
    me += minTimestamp + "-" + maxTimestamp + " ";
    me += processState + ": ";
    me += processedJobFiles + " job files in ";
    me += processFile;
    me += " minJobId: " + minJobId;
    me += " maxJobId: " + maxJobId;

    return me;
  }

  /**
   * The minimum job ID in this batch of jobs. Used to efficiently scan the RAW
   * table. Will not be available until {@link #getProcessState()} is in
   * {@link ProcessState#PREPROCESSED} state or later. In other words, could be
   * null when in {@link ProcessState#CREATED} state.
   */
  public String getMinJobId() {
    return minJobId;
  }

  /**
   * The maximum job ID in this batch of jobs. Used to efficiently scan the RAW
   * table. Will not be available until {@link #getProcessState()} is in
   * {@link ProcessState#PREPROCESSED} state or later. In other words, could be
   * null when in {@link ProcessState#CREATED} state.
   */
  public String getMaxJobId() {
    return maxJobId;
  }

}
