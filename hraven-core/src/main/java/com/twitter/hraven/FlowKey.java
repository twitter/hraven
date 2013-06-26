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

//Leaving comparable as a raw due to sub-typing/overriding issues.
@SuppressWarnings("rawtypes")
public class FlowKey implements Comparable {
  /**
   * The cluster on which the job ran.
   */
  protected final String cluster;
  /**
   * Who ran the final map-reduce flow on Hadoop.
   */
  protected final String userName;
  /**
   * Identifying an application, which can go through different versions.
   */
  protected final String appId;
  /**
   * Identifying one single run of a version of an app. Smaller values indicate
   * a later run. We're using an inverted timestamp Long.MAXVALUE -
   * timstampMillis (milliseconds since January 1, 1970 UTC)
   */
  protected long runId;

  @JsonCreator
  public FlowKey(@JsonProperty("cluster") String cluster,
                 @JsonProperty("userName") String userName,
                 @JsonProperty("appId") String appId,
                 @JsonProperty("runId") long runId) {
    this.cluster = cluster;
    this.runId = runId;
    this.userName = (null == userName) ? Constants.UNKNOWN : userName.trim();
    this.appId = (null == appId) ? Constants.UNKNOWN : appId.trim();
  }

  public FlowKey(FlowKey toCopy) {
    this(toCopy.getCluster(), toCopy.getUserName(), toCopy.getAppId(), toCopy.getRunId());
  }

  /**
   * @return The cluster on which the job ran.
   */
  public String getCluster() {
    return cluster;
  }

  /**
   * @return Who ran the final map-reduce flow on Hadoop.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @return The thing that identifies an application, such as Pig script
   *         identifier, or Scalding identifier.
   */
  public String getAppId() {
    return appId;
  }

  /**
   * Inverted version of {@link JobKey#getRunId()}
   * used in the byte representation for reverse chronological sorting.
   * @return
   */
  public long getEncodedRunId() {
    return encodeRunId(runId);
  }

  /**
   * Encodes the given timestamp for ordering by run ID
   */
  public static long encodeRunId(long timestamp) {
    return Long.MAX_VALUE - timestamp;
  }

  /**
   * @return Identifying one single run of a version of an app. A smaller value
   *         should indicate a later run.
   */
  public long getRunId() {
    return runId;
  }

  /**
   * Compares two FlowKey objects on the basis of
   * their cluster, userName, appId and encodedRunId
   *
   * @param other
   * @return 0 if this cluster, userName, appId and encodedRunId are equal to
   * 		    	 the other's cluster, userName, appId and encodedRunId,
   * 		 1 if this cluster or userName or appId or encodedRunId are less than
   * 		    	the other's cluster, userName, appId and encodedRunId,
   * 		 -1 if this cluster and userName and appId and encodedRunId are greater
   * 		    	the other's cluster, userName, appId and encodedRunId,
   *
   */
  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    FlowKey otherKey = (FlowKey)other;
    return new CompareToBuilder().append(this.cluster, otherKey.getCluster())
        .append(this.userName, otherKey.getUserName())
        .append(this.appId, otherKey.getAppId())
        .append(getEncodedRunId(), otherKey.getEncodedRunId())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FlowKey) {
      return compareTo((FlowKey)other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder()
          .append(this.cluster)
          .append(this.userName)
          .append(this.appId)
          .append(getEncodedRunId())
          .toHashCode();
  }

}
