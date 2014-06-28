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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * The job ID should be relatively unique, unless two clusters start at the same
 * time. However, given a jobId it is not immediately clear which cluster a job
 * ran on (unless the cluster has not been restarted and the prefix is still the
 * current one). This class represents the fully qualified job identifier.
 * 
 */
public class QualifiedJobId extends JobId implements Writable {

  /**
   * The Hadoop cluster on which the job ran.
   */
  private String cluster;

  /**
   * Constructor.
   * 
   * @param cluster
   * @param jobId
   */
  @JsonCreator
  public QualifiedJobId(@JsonProperty("cluster") String cluster,
                        @JsonProperty("jobId") String jobId) {
    super(jobId);
    this.cluster = (cluster != null ? cluster.trim() : "");
  }

  public QualifiedJobId(String cluster, JobId jobId) {
    super(jobId);
    this.cluster = (cluster != null ? cluster.trim() : "");
  }

  /**
   * @return The Hadoop cluster on which the job ran.
   */
  public String getCluster() {
    return cluster;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, this.cluster);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.cluster = Text.readString(in);
  }

}
