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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.twitter.hraven.datasource.JobHistoryService;

/**
 * Captures the details of tasks for a hadoop job
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class TaskDetails implements Comparable<TaskDetails> {

  private static Log LOG = LogFactory.getLog(TaskDetails.class);

  private TaskKey taskKey;

  // task-level stats
  private String taskId;
  private String type;
  private String status;
  private String[] splits;
  private long startTime;
  private long finishTime;

  // task-level counters
  private CounterMap counters = new CounterMap();

  // task attempt specific fields
  private String taskAttemptId;
  private String trackerName;
  private int httpPort;
  private String hostname;
  private String state;
  private String error;
  private long shuffleFinished;
  private long sortFinished;

  @JsonCreator
  public TaskDetails(@JsonProperty("taskKey") TaskKey taskKey) {
    this.taskKey = taskKey;
  }

  public TaskKey getTaskKey() {
    return taskKey;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String[] getSplits() {
    return splits;
  }

  public void setSplits(String[] splits) {
    this.splits = splits;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public CounterMap getCounters() {
    return this.counters;
  }

  /**
   * Compares two TaskDetails objects on the basis of their TaskKey
   *
   * @param other
   * @return 0 if this TaskKey is equal to the other TaskKey,
   * 		 1 if this TaskKey greater than other TaskKey,
   * 		-1 if this TaskKey is less than other TaskKey
   *
   */
  @Override
  public int compareTo(TaskDetails otherTask) {
    if (otherTask == null) {
      return -1;
    }

    return new CompareToBuilder().append(this.taskKey, otherTask.getTaskKey())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskDetails ) {
      return compareTo((TaskDetails)other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder()
          .append(this.taskKey)
          .toHashCode();
  }

  /* *** Task attempt properties  *** */

  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(String taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  public String getTrackerName() {
    return trackerName;
  }

  public void setTrackerName(String trackerName) {
    this.trackerName = trackerName;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public long getShuffleFinished() {
    return shuffleFinished;
  }

  public void setShuffleFinished(long shuffleFinished) {
    this.shuffleFinished = shuffleFinished;
  }

  public long getSortFinished() {
    return sortFinished;
  }

  public void setSortFinished(long sortFinished) {
    this.sortFinished = sortFinished;
  }

  /**
   * Looks through the hbase result's map of task details
   * and populates fields of {@link TaskDetails}
   * @param taskValues
   */
  public void populate(Map<byte[],byte[]> taskValues) {

    this.taskId = JobDetails.getValueAsString(JobHistoryKeys.TASKID, taskValues);
    if(StringUtils.isBlank(taskId)) {
      this.taskId = this.taskKey.getTaskId();
    }
    LOG.debug(" in populate in task details " + this.taskId);

    this.type = JobDetails.getValueAsString(JobHistoryKeys.TASK_TYPE, taskValues);
    this.status = JobDetails.getValueAsString(JobHistoryKeys.TASK_STATUS, taskValues);
    String taskSplits = JobDetails.getValueAsString(JobHistoryKeys.SPLITS, taskValues);
    if (taskSplits != null) {
      this.splits = taskSplits.split(",");
    }
    this.startTime = JobDetails.getValueAsLong(JobHistoryKeys.START_TIME, taskValues);
    this.finishTime = JobDetails.getValueAsLong(JobHistoryKeys.FINISH_TIME, taskValues);
    this.taskAttemptId = JobDetails.getValueAsString(JobHistoryKeys.TASK_ATTEMPT_ID, taskValues);
    this.trackerName = JobDetails.getValueAsString(JobHistoryKeys.TRACKER_NAME, taskValues);
    this.httpPort = JobDetails.getValueAsInt(JobHistoryKeys.HTTP_PORT, taskValues);
    this.hostname = JobDetails.getValueAsString(JobHistoryKeys.HOSTNAME, taskValues);
    this.state = JobDetails.getValueAsString(JobHistoryKeys.STATE_STRING, taskValues);
    this.error = JobDetails.getValueAsString(JobHistoryKeys.ERROR, taskValues);
    this.shuffleFinished = JobDetails.getValueAsLong(JobHistoryKeys.SHUFFLE_FINISHED, taskValues);
    this.sortFinished = JobDetails.getValueAsLong(JobHistoryKeys.SORT_FINISHED, taskValues);

    // populate task counters
    this.counters = JobHistoryService.parseCounters(
        Constants.COUNTER_COLUMN_PREFIX_BYTES, taskValues);
  }
}
