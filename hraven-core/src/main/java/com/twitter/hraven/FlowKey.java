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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class FlowKey extends AppKey implements WritableComparable<Object> {

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
    super(cluster, userName, appId);
    this.runId = runId;
  }

  public FlowKey(FlowKey toCopy) {
    this(toCopy.getCluster(), toCopy.getUserName(), toCopy.getAppId(), toCopy.getRunId());
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

  public String toString() {
    return super.toString() + Constants.SEP + this.getRunId();
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
    return new CompareToBuilder()
        .appendSuper(super.compareTo(other))
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
          .appendSuper(super.hashCode())
          .append(getEncodedRunId())
          .toHashCode();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    new LongWritable(this.runId).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    LongWritable lw = new LongWritable();
    lw.readFields(in);
    this.runId = lw.get();
  }
}
