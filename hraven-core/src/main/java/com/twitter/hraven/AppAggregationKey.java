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
   */
  private long runId;

  public AppAggregationKey(String cluster, String userName, String appId, Long ts) {
    super(cluster, userName, appId);
    this.setRunId(ts);
  }

  public long getRunId() {
    return runId;
  }

  public void setRunId(long runId) {
    this.runId = runId;
  }

  /**
   * Encodes the given timestamp for ordering by run ID
   */
  public static long encodeRunId(long timestamp) {
    return Long.MAX_VALUE - timestamp;
  }

  /**
   * Inverted version of {@link AppAggregationKey#getRunId()} used in the byte representation for
   * reverse chronological sorting.
   * @return
   */
  public long getEncodedRunId() {
    return encodeRunId(runId);
  }

}
