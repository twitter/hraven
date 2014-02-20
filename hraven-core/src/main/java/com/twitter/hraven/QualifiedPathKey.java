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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * The cluster + path qualifier for hdfs stats
 * this does not change over time
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class QualifiedPathKey implements Comparable<Object> {

  /**
   * The Hadoop cluster name for this hdfs
   */
  private final String cluster;

  /**
   * The hdfs path.
   */
  private final String path;

  /**
   * Constructor.
   *
   * @param cluster
   * @param path
   */
  public QualifiedPathKey(String cluster, String path) {
    this.cluster = (cluster != null ? cluster.trim() : "");
    this.path = (path != null ? path.trim() : "");
  }

  /**
   * @return The Hadoop cluster on which the path exists
   */
  public String getCluster() {
    return cluster;
  }

  /**
   * @return The path
   */
  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    if (StringUtils.isBlank(this.cluster) || StringUtils.isBlank(this.path)) {
      return "";
    }
    return this.cluster + HdfsConstants.SEP + this.path;
  }

  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    QualifiedPathKey otherKey = (QualifiedPathKey) other;
    return new CompareToBuilder().append(this.cluster, otherKey.getCluster())
        .append(getPath(), otherKey.getPath())
        .toComparison();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.cluster)
        .append(this.path)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof QualifiedPathKey) {
      return compareTo((QualifiedPathKey)other) == 0;
    }
    return false;
  }
}
