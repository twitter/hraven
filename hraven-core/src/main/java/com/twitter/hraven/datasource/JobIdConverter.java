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

import com.twitter.hraven.JobId;

/**
 */
public class JobIdConverter implements ByteConverter<JobId> {
  @Override
  public byte[] toBytes(JobId jobId) {
    return Bytes.add(Bytes.toBytes(jobId.getJobEpoch()),
        Bytes.toBytes(jobId.getJobSequence()));
  }

  @Override
  public JobId fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length < 16) {
      return null;
    }

    // expect a packed bytes encoding of [8 bytes epoch][8 bytes seq]
    long epoch = Bytes.toLong(bytes, 0);
    long seq = Bytes.toLong(bytes, 8);
    return new JobId(epoch, seq);
  }
}
