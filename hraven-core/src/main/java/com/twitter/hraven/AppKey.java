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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class AppKey implements WritableComparable<Object> {

  /**
   * The cluster on which the application ran
   */
  protected String cluster;
  /**
   * Who ran the application on Hadoop
   */
  protected String userName;

  /**
   * The thing that identifies an application,
   * such as Pig script identifier, or Scalding identifier.
   */
  protected String appId;

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
   * @return Who ran the application
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

  public String toString() {
    return getCluster() + Constants.SEP + getUserName() + Constants.SEP + getAppId();
  }
  /**
   * Compares two AppKey objects on the basis of their cluster, userName, appId and encodedRunId
   * @param other
   * @return 0 if this cluster, userName, appId are equal to the other's
   *                   cluster, userName, appId,
   *         1 if this cluster or userName or appId are less than the other's
   *                   cluster, userName, appId,
   *        -1 if this cluster and userName and appId are greater the other's
   *                   cluster, userName, appId
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

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.cluster);
    Text.writeString(out, this.userName);
    Text.writeString(out, this.appId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.cluster = Text.readString(in);
    this.userName = Text.readString(in);
    this.appId = Text.readString(in);
  }

}
