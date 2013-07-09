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

  /** reduce shuffle bytes in this flow */
  private long reduceShuffleBytes;

  /** duration/runtime for this flow */
  private long duration;

  /** submit time for this flow (submit time of first job) */
  private long submitTime;

  /**  app Version for this flow  */
  private String version ;

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
   * @param other
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

    // set the submit time of the flow to the submit time of the first job
    if ( this.submitTime == 0L ) {
      this.submitTime = job.getSubmitTime();
    }

    this.version = job.getVersion();
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

  public long getDuration() {
    if (this.getJobCount() > 0) {
      this.duration = this.getJobs().get(this.getJobCount() - 1).getFinishTime()
          - this.getJobs().get(0).getLaunchTime();
    }
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
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
}
