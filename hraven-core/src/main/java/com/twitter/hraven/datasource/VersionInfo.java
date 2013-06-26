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
package com.twitter.hraven.datasource;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class VersionInfo implements Comparable<VersionInfo> {

  private String version;
  private long timestamp;

  public VersionInfo(String v, long ts) {
    this.version = v;
    this.timestamp = ts;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Compares two VersionInfo timestamps to order them in reverse chronological
   * order
   *
   * @param other
   * @return 0 if timestamps are equal, 1 if this timestamp less than other
   *         timestamp, -1 if this timestamp is greater than other timestamp
   *
   */
  @Override
  public int compareTo(VersionInfo other) {
    if (this.timestamp == other.timestamp) {
      return 0;
    }
    if (this.timestamp < other.timestamp) {
      return 1;
    }
    return -1;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof VersionInfo) {
      VersionInfo otherVersionInfo = (VersionInfo) other;
      return (this.timestamp == otherVersionInfo.timestamp)
          && (this.version.equals(otherVersionInfo.version));
    }
    return false;
  }

  @Override
  public int hashCode(){
      return new HashCodeBuilder()
          .append(this.timestamp)
          .append(this.version)
          .toHashCode();
  }

}
