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
   * The hadoop 2.0 namespace name.
   */
  private final String namespace;

  /**
   * Constructor.
   *
   * @param cluster
   * @param path
   */
  public QualifiedPathKey(String cluster, String path) {
    this.cluster = (cluster != null ? cluster.trim() : "");
    this.path = (path != null ? path.trim() : "");
    // hadoop1 clusters don't have namespace
    this.namespace = null;
  }

  /**
   * Constructor for federated namespace (hadoop2)
   *
   * @param cluster
   * @param path
   * @param namespace
   */
  public QualifiedPathKey(String cluster, String path, String namespace) {
    this.cluster = (cluster != null ? cluster.trim() : "");
    this.path = (path != null ? path.trim() : "");
    this.namespace = (namespace != null ? namespace.trim() : "");
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

  /**
   * @return the namespace name
   */
  public String getNamespace() {
    return namespace;
  }

  @Override
  public String toString() {
    if (StringUtils.isBlank(this.cluster) || StringUtils.isBlank(this.path)) {
      return "";
    }
    if (StringUtils.isBlank(namespace)) {
      return this.cluster + HdfsConstants.SEP + this.path;
    } else {
      return this.cluster + HdfsConstants.SEP + this.path
          + HdfsConstants.SEP + this.namespace;
    }
  }

  @Override
  public int compareTo(Object other) {
    if (other == null) {
      return -1;
    }
    QualifiedPathKey otherKey = (QualifiedPathKey) other;
    if (StringUtils.isNotBlank(this.namespace)) {
      return new CompareToBuilder()
          .append(this.cluster, otherKey.getCluster())
          .append(getPath(), otherKey.getPath())
          .append(this.namespace, otherKey.getNamespace())
          .toComparison();
    } else {
      return new CompareToBuilder()
        .append(this.cluster, otherKey.getCluster())
        .append(getPath(), otherKey.getPath())
        .toComparison();
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.cluster)
        .append(this.path)
        .append(this.namespace)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof QualifiedPathKey) {
      return compareTo((QualifiedPathKey)other) == 0;
    }
    return false;
  }

  /**
   * returns true if namespace exists in qualified path
   */
  public boolean hasFederatedNS() {
    return StringUtils.isNotBlank(namespace);
  }
}
