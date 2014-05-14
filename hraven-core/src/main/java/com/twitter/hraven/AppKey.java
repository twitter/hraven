/*
Copyright 2014 Twitter, Inc.

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

public class AppKey implements Comparable<Object> {

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

  @JsonCreator
  public AppKey(@JsonProperty("cluster") String cluster, @JsonProperty("userName") String userName,
      @JsonProperty("appId") String appId) {
    this.cluster = cluster;
    this.userName = (null == userName) ? Constants.UNKNOWN : userName.trim();
    this.appId = (null == appId) ? Constants.UNKNOWN : appId.trim();
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
   * Compares two AppKey objects on the basis of their cluster, userName, appId and encodedRunId
   * @param other
   * @return 0 if this cluster, userName, appId are equal to the other's cluster, userName, appId
   *         and encodedRunId, 1 if this cluster or userName or appIdare less than the other's
   *         cluster, userName, appId, -1 if this cluster and userName and appId are greater the
   *         other's cluster, userName, appId
   */
  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    AppKey otherKey = (AppKey) other;
    return new CompareToBuilder()
        .append(this.cluster, otherKey.getCluster())
        .append(this.userName, otherKey.getUserName())
        .append(this.appId, otherKey.getAppId())
        .toComparison();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AppKey) {
      return compareTo((AppKey) other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.cluster)
        .append(this.userName)
        .append(this.appId)
        .toHashCode();
  }

}
