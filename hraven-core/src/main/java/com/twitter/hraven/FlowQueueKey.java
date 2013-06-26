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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Represents the row key for an entry in the flow_queue table.  flow_queue rows are keyed by:
 *  - cluster
 *  - status code
 *  - inverted timestamp
 *  - unique ID
 */
public class FlowQueueKey {
  private final String cluster;
  private final Flow.Status status;
  private final long timestamp;
  private final String flowId;

  public FlowQueueKey(String cluster, Flow.Status status, long timestamp, String flowId) {
    this.cluster = cluster;
    this.status = status;
    this.timestamp = timestamp;
    this.flowId = flowId;
  }

  public String getCluster() {
    return cluster;
  }

  public Flow.Status getStatus() {
    return status;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getFlowId() {
    return flowId;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FlowQueueKey)) {
      return false;
    }
    FlowQueueKey otherKey = (FlowQueueKey)other;
    return new EqualsBuilder().append(this.cluster, otherKey.cluster)
        .append(this.status, otherKey.status)
        .append(this.timestamp, otherKey.timestamp)
        .append(this.flowId, otherKey.flowId)
        .isEquals();
  }

  public String toString() {
    return new ToStringBuilder(this)
        .append(this.cluster)
        .append(this.status)
        .append(this.timestamp)
        .append(this.flowId)
        .toString();
  }
}
