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
import java.util.Map;
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
import org.apache.commons.lang.NotImplementedException;

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
  private HadoopVersion hadoopVersion;
  private String queue;
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
  private long megabyteMillis;
  private double cost;

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

  public HadoopVersion getHadoopVersion() {
    return hadoopVersion;
  }

  public void setHadoopVersion(String hadoopVersion) {
    // the enum.valueOf could throw a NPE or IllegalArgumentException
    this.hadoopVersion = HadoopVersion.valueOf(hadoopVersion);
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

  public long getMegabyteMillis() {
    return megabyteMillis;
  }

  public void setMegabyteMillis(long megabyteMillis) {
    this.megabyteMillis = megabyteMillis;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public void addTask(TaskDetails task) {
    this.tasks.add(task);
  }

  public void addTasks(List<TaskDetails> tasks) {
    this.tasks.addAll(tasks);
  }

  public List<TaskDetails> getTasks() {
    return this.tasks;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
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
  
  /**
   * Do not use, this is for JSON deserialization only.
   * @param newTasks
   */
  @Deprecated
  public void setTasks(List<TaskDetails> newTasks) {
    if ((newTasks != null) && (newTasks.size() > 0)) {
      throw new NotImplementedException("Expected to be invoked only during deserialization "
            + "for empty/null TaskDetails. Deserialization of non-empty TaskDetails should not be done "
            + "in this setter but by implementing a TaskDetails Custom Deserializer in ClientObjectMapper.");
    }
    this.tasks.clear();
  }

  /**
   * return a value from the Map as a Long
   * @param key
   * @param infoValues
   * @return value as Long or 0L
   */
  static Long getValueAsLong(final JobHistoryKeys key, final Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(key));
    if (value != null) {
      try {
      long retValue = Bytes.toLong(value);
      return retValue;
      } catch (NumberFormatException nfe) {
        LOG.error("Caught NFE while converting to long " + nfe.getMessage());
        nfe.printStackTrace();
        return 0L;
      } catch (IllegalArgumentException iae ) {
        // for exceptions like java.lang.IllegalArgumentException:
        // offset (0) + length (8) exceed the capacity of the array: 7
        LOG.error("Caught IAE while converting to long " +  iae.getMessage());
        iae.printStackTrace();
        return 0L;
      }
    } else {
      return 0L;
    }
  }

  /**
   * return a value from the Map as a Long
   * @param key
   * @param infoValues
   * @return value as Long or 0L
   */
  static Long getValueAsLong(final byte[] key, final Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(key);
    if (value != null) {
      try {
      long retValue = Bytes.toLong(value);
      return retValue;
      } catch (NumberFormatException nfe) {
        LOG.error("Caught NFE while converting to long " + nfe.getMessage());
        nfe.printStackTrace();
        return 0L;
      } catch (IllegalArgumentException iae ) {
        // for exceptions like java.lang.IllegalArgumentException:
        // offset (0) + length (8) exceed the capacity of the array: 7
        LOG.error("Caught IAE while converting to long " +  iae.getMessage());
        iae.printStackTrace();
        return 0L;
      }
    } else {
      return 0L;
    }
  }

  /**
   * return a value for that counters from the NavigableMap as a Long
   * @param key
   * @param infoValues
   * @return counter value as Long or 0L
   */
  Long getCounterValueAsLong(final CounterMap counters, final String counterGroupName,
      final String counterName) {
    Counter c1 = counters.getCounter(counterGroupName, counterName);
    if (c1 != null) {
      return c1.getValue();
    } else {
      return 0L;
    }
  }

  /**
   * return a value from the Map as a String
   * @param key
   * @param map of infoValues
   * @return value as a String or ""
   */
  static String getValueAsString(final JobHistoryKeys key,
      final Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(key));
    if (value != null) {
      return Bytes.toString(value);
    } else {
      return "";
    }
  }

  /**
   * return a value from the Map as a Double
   * @param key to be looked up for the value
   * @param infoValues - the map containing the key values
   * @return value as Double or 0.0
   */
  public static double getValueAsDouble(byte[] key, Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(key);
    if (value != null) {
      return Bytes.toDouble(value);
    } else {
      return 0.0;
    }
  }

  /**
   * return an enum value from the NavigableMap for hadoop version
   * @param key
   * @param infoValues
   * @return value as a enum or default of hadoop ONE
   */
  private HadoopVersion getHadoopVersionFromResult(final JobHistoryKeys key,
      final NavigableMap<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(key));
    if (value != null) {
      String hv = Bytes.toString(value);
      // could throw an NPE or IllegalArgumentException
      return HadoopVersion.valueOf(hv);
    } else {
      // default is hadoop 1
      return HadoopVersion.ONE;
    }
  }

  /**
   * get value from a map as an int
   * @param key
   * @param infoValues
   * @return int
   */
  static int getValueAsInt(JobHistoryKeys key, Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(JobHistoryKeys.KEYS_TO_BYTES.get(key));
    if (value != null) {
      try {
        int retValue = Bytes.toInt(value);
        return retValue;
      } catch (NumberFormatException nfe) {
        LOG.error("Caught NFE while converting to int " + nfe.getMessage());
        nfe.printStackTrace();
        return 0;
      } catch (IllegalArgumentException iae) {
        // for exceptions like java.lang.IllegalArgumentException:
        // offset (0) + length (8) exceed the capacity of the array: 7
        LOG.error("Caught IAE while converting to int " + iae.getMessage());
        iae.printStackTrace();
        return 0;
      }
    } else {
      return 0;
    }
  }

  /**
   * return a value from the result as a String
   * @param key
   * @param infoValues
   * @return value as a String or ""
   */
  static String getValueAsString(final byte[] key, final NavigableMap<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(key);
    if (value != null) {
      return Bytes.toString(value);
    } else {
      return "";
    }
  }

  /** TODO: refactor this out into a data access layer */
  public void populate(Result result) {
    // process job-level stats and properties
    NavigableMap<byte[], byte[]> infoValues = result.getFamilyMap(Constants.INFO_FAM_BYTES);

    this.jobId = getValueAsString(JobHistoryKeys.JOBID, infoValues);
    this.user = getValueAsString(JobHistoryKeys.USER, infoValues);
    this.jobName = getValueAsString(JobHistoryKeys.JOBNAME, infoValues);
    this.priority = getValueAsString(JobHistoryKeys.JOB_PRIORITY, infoValues);
    this.status = getValueAsString(JobHistoryKeys.JOB_STATUS, infoValues);
    this.hadoopVersion = getHadoopVersionFromResult(JobHistoryKeys.hadoopversion, infoValues);
    this.version = getValueAsString(Constants.VERSION_COLUMN_BYTES, infoValues);
    this.cost = getValueAsDouble(Constants.JOBCOST_BYTES, infoValues);

    // times
    this.submitTime = getValueAsLong(JobHistoryKeys.SUBMIT_TIME, infoValues);
    this.launchTime = getValueAsLong(JobHistoryKeys.LAUNCH_TIME, infoValues);
    this.finishTime = getValueAsLong(JobHistoryKeys.FINISH_TIME, infoValues);
    this.megabyteMillis = getValueAsLong(Constants.MEGABYTEMILLIS_BYTES, infoValues);

    // task counts
    this.totalMaps = getValueAsLong(JobHistoryKeys.TOTAL_MAPS, infoValues);
    this.totalReduces = getValueAsLong(JobHistoryKeys.TOTAL_REDUCES, infoValues);
    this.finishedMaps = getValueAsLong(JobHistoryKeys.FINISHED_MAPS, infoValues);
    this.finishedReduces = getValueAsLong(JobHistoryKeys.FINISHED_REDUCES, infoValues);
    this.failedMaps = getValueAsLong(JobHistoryKeys.FAILED_MAPS, infoValues);
    this.failedReduces = getValueAsLong(JobHistoryKeys.FAILED_REDUCES, infoValues);

    this.config = JobHistoryService.parseConfiguration(infoValues);
    this.queue = this.config.get(Constants.HRAVEN_QUEUE);
    this.counters = JobHistoryService.parseCounters(Constants.COUNTER_COLUMN_PREFIX_BYTES,
        infoValues);
    this.mapCounters = JobHistoryService.parseCounters(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES,
        infoValues);
    this.reduceCounters = JobHistoryService.parseCounters(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES,
        infoValues);

    // populate stats from counters for this job based on
    // hadoop version
    if (this.hadoopVersion == HadoopVersion.TWO) {
      // map file bytes read
      this.mapFileBytesRead = getCounterValueAsLong(this.mapCounters, 
            Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_READ);

      // map file bytes written
      this.mapFileBytesWritten = getCounterValueAsLong(this.mapCounters,
            Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_WRITTEN);

      // reduce file bytes read
      this.reduceFileBytesRead = getCounterValueAsLong(this.reduceCounters,
            Constants.FILESYSTEM_COUNTER_HADOOP2, Constants.FILES_BYTES_READ);

      // hdfs bytes read
      this.hdfsBytesRead = getCounterValueAsLong(this.counters, Constants.FILESYSTEM_COUNTER_HADOOP2,
            Constants.HDFS_BYTES_READ);

      // hdfs bytes written
      this.hdfsBytesWritten = getCounterValueAsLong(this.counters, Constants.FILESYSTEM_COUNTER_HADOOP2,
            Constants.HDFS_BYTES_WRITTEN);

      // map slot millis
      this.mapSlotMillis = getCounterValueAsLong(this.counters, Constants.JOB_COUNTER_HADOOP2,
            Constants.SLOTS_MILLIS_MAPS);

      // reduce slot millis
      this.reduceSlotMillis = getCounterValueAsLong(this.counters, Constants.JOB_COUNTER_HADOOP2,
            Constants.SLOTS_MILLIS_REDUCES);

      // reduce shuffle bytes
      this.reduceShuffleBytes = getCounterValueAsLong(this.reduceCounters, Constants.TASK_COUNTER_HADOOP2,
            Constants.REDUCE_SHUFFLE_BYTES);
    } else { // presume it's hadoop1
      // map file bytes read
      this.mapFileBytesRead = getCounterValueAsLong(this.mapCounters, Constants.FILESYSTEM_COUNTERS,
            Constants.FILES_BYTES_READ);

      // map file bytes written
      this.mapFileBytesWritten = getCounterValueAsLong(this.mapCounters, Constants.FILESYSTEM_COUNTERS,
            Constants.FILES_BYTES_WRITTEN);

      // reduce file bytes read
      this.reduceFileBytesRead = getCounterValueAsLong(this.reduceCounters, Constants.FILESYSTEM_COUNTERS,
            Constants.FILES_BYTES_READ);

      // hdfs bytes read
      this.hdfsBytesRead = getCounterValueAsLong(this.counters, Constants.FILESYSTEM_COUNTERS,
            Constants.HDFS_BYTES_READ);

      // hdfs bytes written
      this.hdfsBytesWritten = getCounterValueAsLong(this.counters, Constants.FILESYSTEM_COUNTERS,
            Constants.HDFS_BYTES_WRITTEN);

      // map slot millis
      this.mapSlotMillis = getCounterValueAsLong(this.counters, Constants.JOBINPROGRESS_COUNTER,
            Constants.SLOTS_MILLIS_MAPS);

      // reduce slot millis
      this.reduceSlotMillis = getCounterValueAsLong(this.counters, Constants.JOBINPROGRESS_COUNTER,
            Constants.SLOTS_MILLIS_REDUCES);

      // reduce shuffle bytes
      this.reduceShuffleBytes = getCounterValueAsLong(this.reduceCounters, Constants.TASK_COUNTER,
            Constants.REDUCE_SHUFFLE_BYTES);
    }
    // populate the task-level data
    // TODO: make sure to properly implement setTasks(...) before adding TaskDetails
    //populateTasks(result.getFamilyMap(Constants.TASK_FAM_BYTES));
  }

}
