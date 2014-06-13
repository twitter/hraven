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

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A flow represents a collection of map reduce jobs run together as a data
 * processing pipeline. In the case of scalding, this would be a given run of a
 * scalding script. In the case of pig, this would be a single run of a pig
 * script.
 */
@JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
public class Flow implements Comparable<Flow> {
  public enum Status { RUNNING('r'), SUCCEEDED('s'), FAILED('f');

    byte[] code;

    Status(char code) {
      this.code = new byte[]{(byte)code};
    }
    public byte[] code() {
      return this.code;
    }
  }

  public static Map<byte[],Status> STATUS_BY_CODE =
      new TreeMap<byte[],Status>(Bytes.BYTES_COMPARATOR);
  static {
    for (Status s : Status.values()) {
      STATUS_BY_CODE.put(s.code(), s);
    }
  }

  private FlowKey key;
  /** Key for this flow in the flow_queue table (may be null) */
  private FlowQueueKey queueKey;

  private List<JobDetails> jobs = new ArrayList<JobDetails>();

  /** Descriptive name for the executing flow */
  private String flowName;

  /** Allow flow user to be set outside of the flow key */
  private String userName;

  /** JSON serialized DAG of the workflow jobs */
  private String jobGraphJSON;

  /** Progress indicator for in-progress flows */
  private int progress;

  /** Number of jobs in this flow */
  private int jobCount;

  /** Number of map tasks in this flow */
  private long totalMaps;

  /** Number of reduce tasks in this flow */
  private long totalReduces;

  /** map file bytes read in this flow */
  private long mapFileBytesRead;

  /** map file bytes written in this flow */
  private long mapFileBytesWritten;

  /** reduce file bytes read in this flow */
  private long reduceFileBytesRead;

  /** HDFS bytes read in this flow */
  private long hdfsBytesRead;

  /** HDFS bytes written in this flow */
  private long hdfsBytesWritten;

  /** map slot millis in this flow */
  private long mapSlotMillis;

  /** reduce slot millis  in this flow */
  private long reduceSlotMillis;

  /** megabyte millis  in this flow */
  private long megabyteMillis;

  /** cost of this flow */
  private double cost;

  /** reduce shuffle bytes in this flow */
  private long reduceShuffleBytes;

  /**
   * duration is the time for which a job executes
   * it is finish time of the job that finishes last
   * in the flow minus
   * the launch time of the first job to get launched
   */
  @Deprecated
  private long duration;

  /**
   * wallClockTime is the time seen by the user on
   * the hadoop cluster
   * finish time of job that finishes last minus submit
   * time of first job in the flow
   */
  private long wallClockTime;

  /** submit time for this flow in milliseconds
   * (submit time of first job)
   */
  private long submitTime;

  /** launch time for this flow in milliseconds
   * (launch time of first job)
   */
  private long launchTime;

  /** finish time for this flow in milliseconds
   * (finish time of the job that completes last)
   */
  private long finishTime;

  /**  app Version for this flow  */
  private String version ;

  /**  hadoop Version for this flow  */
  private HadoopVersion hadoopVersion ;

  /**  hadoop pool/queue for this flow  */
  private String queue ;

  /** Aggregated counters from all jobs in this flow */
  private CounterMap counters = new CounterMap();

  /** Aggregated map counters from all jobs in this flow */
  private CounterMap mapCounters = new CounterMap();

  /** Aggregated reduce counters from all jobs in this flow */
  private CounterMap reduceCounters = new CounterMap();

  /**
   * Constructor
   * 
   * @param key
   */
  @JsonCreator
  public Flow(@JsonProperty("flowKey") FlowKey key) {
    this.key = key;
    // default flow name to appId
    if (this.key != null) {
      this.flowName = this.key.getAppId();
      this.userName = this.key.getUserName();
    }
  }

  /**
   * @param jobKey
   * @return whether the given jobKey is part of this flow.
   */
  public boolean contains(JobKey jobKey) {
    if (jobKey == null || this.key == null) {
      return false;
    }

    // No need to check for null because jobKey will not return nulls.
    return key.equals(jobKey);
  }

  /**
   * Compares two Flow objects on the basis of their FlowKeys
   *
   * @param otherFlow
   * @return 0 if the FlowKeys are equal,
   * 		 1 if this FlowKey greater than other FlowKey,
   * 		-1 if this FlowKey is less than other FlowKey
   *
   */
  @Override
  public int compareTo(Flow otherFlow) {
    if (otherFlow == null) {
      return -1;
    }
    return new CompareToBuilder().append(this.key, otherFlow.getFlowKey())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Flow) {
      return compareTo((Flow)other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder()
          .append(this.key)
          .toHashCode();
  }

  public FlowKey getFlowKey() {
    return key;
  }

  public FlowQueueKey getQueueKey() {
    return queueKey;
  }

  public void setQueueKey(FlowQueueKey queueKey) {
    this.queueKey = queueKey;
  }

  public String getCluster() {
    if (this.key == null) {
      return null;
    }
    return this.key.getCluster();
  }

  public String getAppId() {
    if (this.key == null) {
      return null;
    }
    return this.key.getAppId();
  }

  public long getRunId() {
    if (this.key == null) {
      return 0;
    }
    return this.key.getRunId();
  }

  public List<JobDetails> getJobs() {
    return this.jobs;
  }

  public void addJob(JobDetails job) {
    this.jobs.add(job);
    this.jobCount++;
    this.totalMaps += job.getTotalMaps();
    this.totalReduces += job.getTotalReduces();
    this.hdfsBytesRead += job.getHdfsBytesRead();
    this.hdfsBytesWritten += job.getHdfsBytesWritten();
    this.reduceShuffleBytes += job.getReduceShuffleBytes();
    this.mapFileBytesRead += job.getMapFileBytesRead();
    this.mapFileBytesWritten += job.getMapFileBytesWritten();
    this.reduceFileBytesRead += job.getReduceFileBytesRead();
    this.mapSlotMillis += job.getMapSlotMillis();
    this.reduceSlotMillis += job.getReduceSlotMillis();
    this.megabyteMillis += job.getMegabyteMillis();
    this.cost += job.getCost();

    // set the submit time of the flow to the submit time of the first job
    if (( this.submitTime == 0L ) || (job.getSubmitTime() < this.submitTime)) {
      this.submitTime = job.getSubmitTime();
      // set the hadoop version once for this job
      this.hadoopVersion = job.getHadoopVersion();
      // set the queue/pool once for this flow
      this.queue = job.getQueue();
      if (this.hadoopVersion == null) {
        // set it to default so that we don't run into NPE during json serialization
        this.hadoopVersion = HadoopVersion.ONE;
      }
    }

    // set the launch time of the flow to the launch time of the first job
    if (( this.launchTime == 0L ) || (job.getLaunchTime() < this.launchTime)) {
      this.launchTime = job.getLaunchTime();
    }

    // set the finish time of the flow to the finish time of the last job
    if (job.getFinishTime() > this.finishTime) {
      this.finishTime = job.getFinishTime();
    }

    this.version = job.getVersion();

    // add up all of the job counters
    // total counters
    for (Counter c : job.getCounters()) {
      long prevValue = 0L;
      Counter current = counters.getCounter(c.getGroup(), c.getKey());
      if (current != null) {
        prevValue = current.getValue();
      }
      counters.add(new Counter(c.getGroup(), c.getKey(), c.getValue()+prevValue));
    }

    // map counters
    for (Counter c : job.getMapCounters()) {
      long prevValue = 0L;
      Counter current = mapCounters.getCounter(c.getGroup(), c.getKey());
      if (current != null) {
        prevValue = current.getValue();
      }
      mapCounters.add(new Counter(c.getGroup(), c.getKey(), c.getValue()+prevValue));
    }

    // reduce counters
    for (Counter c : job.getReduceCounters()) {
      long prevValue = 0L;
      Counter current = reduceCounters.getCounter(c.getGroup(), c.getKey());
      if (current != null) {
        prevValue = current.getValue();
      }
      reduceCounters.add(new Counter(c.getGroup(), c.getKey(), c.getValue()+prevValue));
    }
  }

  public String getUserName() {
    return this.userName;
  }

  /**
   * Allows username to be set before a full {@link FlowKey} can be constructed.
   * @param name
   */
  public void setUserName(String name) {
    this.userName = name;
  }

  public String getJobGraphJSON() {
    return jobGraphJSON;
  }

  public void setJobGraphJSON(String json) {
    this.jobGraphJSON = json;
  }

  public String getFlowName() {
    return this.flowName;
  }

  public void setFlowName(String name) {
    this.flowName = name;
  }

  public int getProgress() {
    return this.progress;
  }

  public void setProgress(int progress) {
    this.progress = progress;
  }

  // for JSON deserialiation
  void setJobs(List<JobDetails> jobs) { this.jobs = jobs; }

  public int getJobCount() {
    return jobCount;
  }

  public void setJobCount(int jobCount) {
    this.jobCount = jobCount;
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

  public HadoopVersion getHadoopVersion() {
    return hadoopVersion;
  }

  public void setHadoopVersion(HadoopVersion hadoopVersion) {
    this.hadoopVersion = hadoopVersion;
  }

  public long getReduceShuffleBytes() {
    return reduceShuffleBytes;
  }

  public void setReduceShuffleBytes(long reduceShuffleBytes) {
    this.reduceShuffleBytes = reduceShuffleBytes;
  }

  public long getHdfsBytesRead() {
    return hdfsBytesRead;
  }

  public void setHdfsBytesRead(long hdfsBytesRead) {
    this.hdfsBytesRead = hdfsBytesRead;
  }

  public long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }

  public void setHdfsBytesWritten(long hdfsBytesWritten) {
    this.hdfsBytesWritten = hdfsBytesWritten;
  }

  /**
   * duration is the time for which a job executes
   *
   * Calculated as
   * finish time of the job that finishes last
   * in the flow minus
   * the launch time of the first job to get launched
   * @return duration
   */
  @Deprecated
  public long getDuration() {
    if (this.getJobCount() > 0) {
      this.duration = this.finishTime - this.launchTime;
    }
    return duration;
  }

  @Deprecated
  public void setDuration(long duration) {
    this.duration = duration;
  }

  /**
   * wallClockTime time is the time seen by the user
   * on the hadoop cluster in milliseconds
   *
   * Calculated as:
   * finish time of job that finishes last minus
   * submit time of first job in the flow
   * @return time in milliseconds
   */
  public long getWallClockTime() {
    if (this.getJobCount() > 0) {
      this.wallClockTime = this.finishTime - this.submitTime;
    }
    return wallClockTime;
  }

  /** sets the wallClockTime in milliseconds */
  public void setWallClockTime(long wallClockTime) {
    this.wallClockTime = wallClockTime;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
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

  public void setMapFileBytesWritten(long mapFileBytesWritten) {
    this.mapFileBytesWritten = mapFileBytesWritten;
  }

  public long getReduceFileBytesRead() {
    return reduceFileBytesRead;
  }

  public void setReduceFileBytesRead(long reduceFileBytesRead) {
    this.reduceFileBytesRead = reduceFileBytesRead;
  }

  public CounterMap getCounters() {
    return counters;
  }

  public CounterMap getMapCounters() {
    return mapCounters;
  }

  public CounterMap getReduceCounters() {
    return reduceCounters;
  }
}
