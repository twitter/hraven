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

/**
 * Represents a unique hdfs stats record
 * It corresponds to the row key for a record in hbase
 *
 * Row keys are stored as:
 * encodedRunId!cluster!path
 * where encodedRunId is the inverted timestamp of the
 * top of the hour at which stats were collected
 */
public class HdfsStatsKey implements Comparable<Object> {

  /**
   * Fully qualified cluster + path
   */
  private final QualifiedPathKey pathKey;

  /**
   * inverted timestamp of the top of the hour
   * at which hdfs stats were collected
   */
  private final long encodedRunId;

  /**
   * Constructor for hadoop1
   *
   * @param cluster
   *          the Hadoop cluster for the hdfs stats.
   * @param path
   *          the hdfs path
   * @param encodedRunId
   *          Inverted top of the hour collection timestamp
   */
  @JsonCreator
  public HdfsStatsKey(@JsonProperty("cluster") String cluster,
                @JsonProperty("path") String path,
                @JsonProperty("encodedRunId") long encodedRunId) {

    this(new QualifiedPathKey(cluster, path), encodedRunId);
  }

  /**
   * Constructor for federated namespace (hadoop2)
   *
   * @param cluster
   *          the Hadoop cluster for the hdfs stats
   * @param path
   *          the hdfs path
   * @param namespace
   *          Namespace name
   * @param encodedRunId
   *          Inverted top of the hour collection timestamp
   */
  @JsonCreator
  public HdfsStatsKey(@JsonProperty("cluster") String cluster,
                @JsonProperty("path") String path,
                @JsonProperty("namespace") String namespace,
                @JsonProperty("encodedRunId") long encodedRunId) {

    this(new QualifiedPathKey(cluster, path, namespace), encodedRunId);
  }

  /**
   * Creates a new HdfsStatsKey from the given parameters
   *
   * @param QualifiedPathKey The combined cluster + path
   * @param encodedRunId inverted run timestamp
   */
  @JsonCreator
  public HdfsStatsKey(@JsonProperty("pathKey") QualifiedPathKey pathKey,
                @JsonProperty("encodedRunId") long encodedRunId) {
    this.pathKey = pathKey;
    this.encodedRunId = encodedRunId;
  }

  /**
   * Creates a new HdfsStatsKey from the given HdfsKey
   * @param key
   */
  public HdfsStatsKey(HdfsStatsKey key) {
    this.pathKey = key.pathKey;
    this.encodedRunId = key.encodedRunId;
  }

  /**
   * returns the run id based on the encoded run id
   */
  public static long getRunId(long encodedRunId) {
    return Long.MAX_VALUE - encodedRunId;
  }

  /**
   * returns the run id based on the encoded run id
   */
  public long getRunId() {
    return Long.MAX_VALUE - this.encodedRunId;
  }

  /**
   * @return The fully qualified cluster + path
   */
  public QualifiedPathKey getQualifiedPathKey() {
    return pathKey;
  }

  public long getEncodedRunId() {
    return this.encodedRunId;
  }

  public boolean hasFederatedNS() {
    return this.pathKey.hasFederatedNS();
  }

  @Override
  public String toString() {
    if (this.pathKey == null) {
      return "";
    }
    return this.encodedRunId +  HdfsConstants.SEP + this.pathKey.toString();
  }

  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    HdfsStatsKey otherKey = (HdfsStatsKey) other;
    return new CompareToBuilder()
        .append(this.pathKey, otherKey.getQualifiedPathKey())
        .append(this.encodedRunId, otherKey.getEncodedRunId())
        .toComparison();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.pathKey)
        .append(this.encodedRunId)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HdfsStatsKey) {
      return compareTo((HdfsStatsKey) other) == 0;
    }
    return false;
  }
}