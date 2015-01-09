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

/**
 * Represents the row key that stores Aggregations for an app
 */
public class AppAggregationKey extends AppKey {

  /**
   * timestamp stored as part of row key in aggregation table
   * this is usually top of the day or top of the week timestamp
   * All apps that belong to that day (or that week for weekly aggregations)
   * have the same aggregation id
   *
   * If a {@link Flow} (like a pig job or a scalding job) spans more than a 1 day,
   * the aggregationId is the day that the first job in that Flow started running,
   * which is the submitTime or runId of that {@link Flow}
   */
  private long aggregationId;

  public AppAggregationKey(String cluster, String userName, String appId, Long ts) {
    super(cluster, userName, appId);
    this.setAggregationId(ts);
  }

  public long getAggregationId() {
    return aggregationId;
  }

  public void setAggregationId(long aggregationId) {
    this.aggregationId = aggregationId;
  }

  /**
   * Encodes the given timestamp for ordering by run ID
   */
  public static long encodeAggregationId(long timestamp) {
    return Long.MAX_VALUE - timestamp;
  }

  /**
   * Inverted version of {@link AppAggregationKey#getaggregationId()} used in the byte representation for
   * reverse chronological sorting.
   * @return
   */
  public long getEncodedAggregationId() {
    return encodeAggregationId(aggregationId);
  }

}
