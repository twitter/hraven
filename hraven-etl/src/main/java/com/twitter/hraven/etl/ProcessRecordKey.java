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
package com.twitter.hraven.etl;

/**
 */
public class ProcessRecordKey {
  private final String cluster;
  private final long timestamp;

  public ProcessRecordKey(String cluster, long timestamp) {
    this.cluster = cluster;
    this.timestamp = timestamp;
  }

  public String getCluster() {
    return cluster;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ProcessRecordKey) {
      return cluster.equals(((ProcessRecordKey) other).getCluster()) &&
          timestamp == ((ProcessRecordKey) other).getTimestamp();
    }
    return false;
  }

  public String toString() {
    return new StringBuilder("ProcessRecordKey[cluster=")
        .append(cluster)
        .append(", timestamp=")
        .append(timestamp)
        .append("]")
        .toString();
  }
}
