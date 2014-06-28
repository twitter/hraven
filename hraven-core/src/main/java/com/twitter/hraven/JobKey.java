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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Represents the row key for a given job. Row keys are stored as: username !
 * appid ! version ! runid ! jobid
 */
public class JobKey extends FlowKey implements WritableComparable<Object>{

  /**
   * Fully qualified cluster + parsed job identifier
   */
  private final QualifiedJobId jobId;

  /**
   * Constructor.
   *
   * @param cluster
   *          the Hadoop cluster on which the job ran.
   * @param userName
   *          the Hadoop user name that ran a job
   * @param appId
   *          The thing that identifies an application, such as Pig script
   *          identifier, or Scalding identifier.
   * @param runId
   *          The identifier that ties the various runs for this job together
   * @param jobId
   *          The Hadoop generated MapReduce JobID.
   */
  public JobKey(String cluster, String userName, String appId, long runId,
      String jobId) {
    // TODO: Change contract to allow for nulls and advertise strings.
    this(new QualifiedJobId(cluster, jobId), userName, appId, runId);
  }

  @JsonCreator
  public JobKey(@JsonProperty("cluster") String cluster,
                @JsonProperty("userName") String userName,
                @JsonProperty("appId") String appId,
                @JsonProperty("runId") long runId,
                @JsonProperty("jobId") JobId jobId) {
    this(new QualifiedJobId(cluster, jobId), userName, appId, runId);
  }

  /**
   * Creates a new JobKey from the given parameters
   *
   * @param qualifiedJobId The combined cluster + job ID
   * @param userName The user name that ran the job
   * @param appId The application identifier
   * @param runId The run timestamp
   */
  public JobKey(QualifiedJobId qualifiedJobId, String userName, String appId,
      long runId) {
    super(qualifiedJobId.getCluster(), userName, appId, runId);
    this.jobId = qualifiedJobId;
  }

  /**
   * Constructor.
   *
   * @param jobDesc
   *          from which to construct this JobKey.
   */
  public JobKey(JobDesc jobDesc) {
    this(jobDesc.getCluster(), jobDesc.getUserName(), jobDesc.getAppId(),
        jobDesc.getRunId(), jobDesc.getJobId());
  }

  /**
   * @return The fully qualified cluster + parsed job ID
   */
  public QualifiedJobId getQualifiedJobId() {
    return jobId;
  }

  /**
   * @return The Hadoop map-reduce Job identifier as run on the JobTracker.
   */
  public JobId getJobId() {
    return jobId;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return super.toString() + Constants.SEP + this.jobId.getJobIdString();
  }
  /**
   * Compares two JobKey QualifiedJobId
   *
   * @param other
   * @return 0 if the Qualified Job Ids are equal,
   * 		 1 if this QualifiedJobId greater than other QualifiedJobId,
   * 		-1 if this QualifiedJobId is less than other QualifiedJobId
   */
  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    JobKey otherKey = (JobKey)other;
    return new CompareToBuilder().appendSuper(super.compareTo(otherKey))
        .append(this.jobId, otherKey.getJobId())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof JobKey) {
      return compareTo((JobKey)other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder().appendSuper(super.hashCode())
          .append(this.jobId)
          .toHashCode();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.jobId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.jobId.readFields(in);
  }

}
