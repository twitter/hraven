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
package com.twitter.hraven;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;


/**
 * Represents the row key for an individual job task.  This key shares all the
 * same components from the job key, with the additional of the task ID:
 * <pre>
 *   (m|r)_tasknumber(_attemptnumber)?
 * </pre>
 */
@JsonSerialize(
    include=JsonSerialize.Inclusion.NON_NULL
  )
public class TaskKey extends JobKey implements Comparable<Object> {
  private String taskId;

  @JsonCreator
  public TaskKey(@JsonProperty("jobId") JobKey jobKey, @JsonProperty("taskId") String taskId) {
    super(jobKey.getQualifiedJobId(), jobKey.getUserName(), jobKey.getAppId(),
        jobKey.getRunId());
    this.taskId = taskId;
  }

  public String getTaskId() {
    return this.taskId;
  }

  public String toString() {
    return new StringBuilder(super.toString())
        .append(Constants.SEP).append(taskId).toString();
  }

  /**
   * Compares two TaskKey objects on the basis of their taskId
   *
   * @param other
   * @return 0 if the taskIds are equal,
   * 		 1 if this taskId is greater than other taskId,
   * 		-1 if this taskId is less than other taskId
   */
  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    TaskKey otherKey = (TaskKey) other;
    return new CompareToBuilder().appendSuper(super.compareTo(otherKey))
        .append(this.taskId, otherKey.getTaskId())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskKey) {
      return compareTo((TaskKey)other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
	  return new HashCodeBuilder().appendSuper(super.hashCode())
          .append(this.taskId)
          .toHashCode();
  }
}
