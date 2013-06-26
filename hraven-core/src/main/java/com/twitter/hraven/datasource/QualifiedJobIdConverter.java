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

import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobId;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.util.ByteUtil;

/**
 */
public class QualifiedJobIdConverter implements ByteConverter<QualifiedJobId> {
  JobIdConverter jobIdConv = new JobIdConverter();

  @Override
  public byte[] toBytes(QualifiedJobId id) {
    return ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(id.getCluster()),
        jobIdConv.toBytes(id));
  }

  @Override
  public QualifiedJobId fromBytes(byte[] bytes) {
    byte[][] parts = ByteUtil.split(bytes, Constants.SEP_BYTES, 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid encoded ID, must be 2 parts");
    }
    String cluster = Bytes.toString(parts[0]);
    JobId jobId = jobIdConv.fromBytes(parts[1]);
    return new QualifiedJobId(cluster, jobId);
  }
}
