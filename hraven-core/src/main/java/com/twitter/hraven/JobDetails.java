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
package com.twitter.hraven;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.twitter.hraven.datasource.JobHistoryService;

/**
 * Represents the configuration, statistics, and counters from a single
 * map reduce job.  Individual task details are also nested, though may not
 * be loaded in all cases.
 */
@JsonSerialize(
  include= JsonSerialize.Inclusion.NON_NULL
)
public class JobDetails implements Comparable<JobDetails> {
  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(JobDetails.class);

  // job key -- maps to row key
  private JobKey jobKey;

  // unique job ID assigned by job tracker
  private String jobId;

  // job-level stats
  private String jobName;
  private String user;
  private String priority;
  private String status;
  private String version;
  private long submitTime;
  private long launchTime;
  private long finishTime;
  private long totalMaps;
  private long totalReduces;
  private long finishedMaps;
  private long finishedReduces;
  private long failedMaps;
  private long failedReduces;
  private long mapFileBytesRead;
  private long mapFileBytesWritten;
  private long reduceFileBytesRead;
  private long hdfsBytesRead;
  private long hdfsBytesWritten;
  private long mapSlotMillis;
  private long reduceSlotMillis;
  private long reduceShuffleBytes;

  // job config
  private Configuration config;

  // job-level counters
  private CounterMap counters = new CounterMap();
  private CounterMap mapCounters = new CounterMap();
  private CounterMap reduceCounters = new CounterMap();

  // tasks
  private List<TaskDetails> tasks = new ArrayList<TaskDetails>();

  @JsonCreator
  public JobDetails(@JsonProperty("jobKey") JobKey key) {
    this.jobKey = key;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof JobDetails) {
      return compareTo((JobDetails)other) == 0;
    }
    return false;
  }

  /**
   * Compares two JobDetails objects on the basis of their JobKey
   *
   * @param other
   * @return 0 if this JobKey is equal to the other JobKey,
   * 		 1 if this JobKey greater than other JobKey,
   * 		-1 if this JobKey is less than other JobKey
   *
   */
  @Override
  public int compareTo(JobDetails otherJob) {
    if (otherJob == null) {
      return -1;
    }
    return new CompareToBuilder().append(this.jobKey, otherJob.getJobKey())
        .toComparison();
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder()
          .append(this.jobKey)
          .toHashCode();
  }

  public JobKey getJobKey() {
    return this.jobKey;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public Date getSubmitDate() {
    return new Date(this.submitTime);
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }

  public Date getLaunchDate() {
    return new Date(this.launchTime);
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public Date getFinishDate() {
    return new Date(this.finishTime);
  }

  /**
   * Returns the elapsed run time for this job (finish time minus launch time).
   * @return
   */
  public long getRunTime() {
    return finishTime - launchTime;
  }

  public long getTotalMaps() {
    return totalMaps;
  }

  public void setTotalMaps(long totalMaps) {
    this.totalMaps = totalMaps;
  }

  public long getTotalReduces() {
    return totalReduces;
  }

  public void setTotalReduces(long totalReduces) {
    this.totalReduces = totalReduces;
  }

  public long getFinishedMaps() {
    return finishedMaps;
  }

  public void setFinishedMaps(long finishedMaps) {
    this.finishedMaps = finishedMaps;
  }

  public long getFinishedReduces() {
    return finishedReduces;
  }

  public void setFinishedReduces(long finishedReduces) {
    this.finishedReduces = finishedReduces;
  }

  public long getFailedMaps() {
    return failedMaps;
  }

  public void setFailedMaps(long failedMaps) {
    this.failedMaps = failedMaps;
  }

  public long getFailedReduces() {
    return failedReduces;
  }

  public void setFailedReduces(long failedReduces) {
    this.failedReduces = failedReduces;
  }

  public long getMapFileBytesRead() {
    return mapFileBytesRead;
  }

  public void setMapFileBytesRead(long mapFileBytesRead) {
    this.mapFileBytesRead = mapFileBytesRead;
  }

  public long getMapFileBytesWritten() {
    return mapFileBytesWritten;
  }

  public void setMapFileBytesWritten(long mapBytesWritten) {
    this.mapFileBytesWritten = mapBytesWritten;
  }

  public long getHdfsBytesRead() {
    return hdfsBytesRead;
  }

  public long getMapSlotMillis() {
    return mapSlotMillis;
  }

  public void setMapSlotMillis(long mapSlotMillis) {
    this.mapSlotMillis = mapSlotMillis;
  }

  public long getReduceSlotMillis() {
    return reduceSlotMillis;
  }

  public void setReduceSlotMillis(long reduceSlotMillis) {
    this.reduceSlotMillis = reduceSlotMillis;
  }

  public long getReduceShuffleBytes() {
    return reduceShuffleBytes;
  }

  public void setReduceShuffleBytes(long reduceShuffleBytes) {
    this.reduceShuffleBytes = reduceShuffleBytes;
  }

  public long getReduceFileBytesRead() {
    return reduceFileBytesRead;
  }

  public void setReduceFileBytesRead(long reduceFileBytesRead) {
    this.reduceFileBytesRead = reduceFileBytesRead;
  }

  public long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }

  public void setHdfsBytesWritten(long hdfsBytesWritten) {
    this.hdfsBytesWritten = hdfsBytesWritten;
  }

  public void setHdfsBytesRead(long hdfsBytesRead) {
    this.hdfsBytesRead = hdfsBytesRead;
  }

  public void addTask(TaskDetails task) {
    this.tasks.add(task);
  }

  public List<TaskDetails> getTasks() {
    return this.tasks;
  }

  public Configuration getConfiguration() {
    return this.config;
  }

  public CounterMap getCounters() {
    return this.counters;
  }

  public CounterMap getMapCounters() {
    return this.mapCounters;
  }

  public CounterMap getReduceCounters() {
    return this.reduceCounters;
  }

  // for JSON deserialization
  void setConfiguration(Configuration config) { this.config = config; }
  void setCounters(CounterMap counters) { this.counters = counters; }
  void setMapCounters(CounterMap mapCounters) { this.mapCounters = mapCounters; }
  void setReduceCounters(CounterMap reduceCounters) { this.reduceCounters = reduceCounters; }

  /** TODO: refactor this out into a data access layer */
  public void populate(Result result) {
    // process job-level stats and properties
    NavigableMap<byte[],byte[]> infoValues = result.getFamilyMap(Constants.INFO_FAM_BYTES);
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBID))) {
      this.jobId = Bytes.toString(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBID)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.USER))) {
      this.user = Bytes.toString(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.USER)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBNAME))) {
      this.jobName = Bytes.toString(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBNAME)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_PRIORITY))) {
      this.priority = Bytes.toString(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_PRIORITY)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_STATUS))) {
      this.status = Bytes.toString(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_STATUS)));
    }
    if (infoValues.containsKey(Constants.VERSION_COLUMN_BYTES)) {
      this.version = Bytes.toString(infoValues.get(Constants.VERSION_COLUMN_BYTES));
    }

    // times
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.SUBMIT_TIME))) {
      this.submitTime = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.SUBMIT_TIME)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.LAUNCH_TIME))) {
      this.launchTime = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.LAUNCH_TIME)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISH_TIME))) {
      this.finishTime = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISH_TIME)));
    }

    // task counts
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_MAPS))) {
      this.totalMaps = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_MAPS)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_REDUCES))) {
      this.totalReduces = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_REDUCES)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_MAPS))) {
      this.finishedMaps = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_MAPS)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_REDUCES))) {
      this.finishedReduces = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISHED_REDUCES)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_MAPS))) {
      this.failedMaps = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_MAPS)));
    }
    if (infoValues.containsKey(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_REDUCES))) {
      this.failedReduces = Bytes.toLong(
          infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FAILED_REDUCES)));
    }

    this.config = JobHistoryService.parseConfiguration(infoValues);
    this.counters = JobHistoryService.parseCounters(
        Constants.COUNTER_COLUMN_PREFIX_BYTES, infoValues);
    this.mapCounters = JobHistoryService.parseCounters(
        Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, infoValues);
    this.reduceCounters = JobHistoryService.parseCounters(
        Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, infoValues);

    // populate stats from counters for this job
    // map file bytes read
    if (this.mapCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ) != null) {
      this.mapFileBytesRead =
          this.mapCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ)
              .getValue();
    }

    // map file bytes written
    if (this.mapCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_WRITTEN) != null) {
      this.mapFileBytesWritten =
          this.mapCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_WRITTEN)
              .getValue();
    }

    // reduce file bytes read
    if (this.reduceCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ) != null) {
      this.reduceFileBytesRead =
          this.reduceCounters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.FILES_BYTES_READ)
              .getValue();
    }

    // hdfs bytes read
    if (this.counters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_READ) != null) {
      this.hdfsBytesRead =
          this.counters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_READ)
              .getValue();
    }

    // hdfs bytes written
    if (this.counters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_WRITTEN) != null) {
      this.hdfsBytesWritten =
          this.counters.getCounter(Constants.FILESYSTEM_COUNTERS, Constants.HDFS_BYTES_WRITTEN)
              .getValue();
    }

    // map slot millis
    if (this.counters.getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_MAPS) != null) {
      this.mapSlotMillis =
          this.counters.getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_MAPS)
              .getValue();
    }

    // reduce slot millis
    if (this.counters.getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_REDUCES) != null) {
      this.reduceSlotMillis =
          this.counters.getCounter(Constants.JOBINPROGRESS_COUNTER, Constants.SLOTS_MILLIS_REDUCES)
              .getValue();
    }

    // reduce shuffle bytes
    if (this.reduceCounters.getCounter(Constants.TASK_COUNTER, Constants.REDUCE_SHUFFLE_BYTES) != null) {
      this.reduceShuffleBytes =
          this.reduceCounters.getCounter(Constants.TASK_COUNTER, Constants.REDUCE_SHUFFLE_BYTES)
              .getValue();
    }

    // populate the task-level data
    //populateTasks(result.getFamilyMap(Constants.TASK_FAM_BYTES));
  }

}
