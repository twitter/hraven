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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobId;

/**
 */
public class JobIdConverter implements ByteConverter<JobId> {
  @Override
  public byte[] toBytes(JobId jobId) {
    String prefix = jobId.getJobPrefix();
    if ((StringUtils.isNotBlank(prefix) && (JobId.JOB_PREFIX.equalsIgnoreCase(prefix)))
        || (StringUtils.isBlank(prefix))) {
      // do not include "job" prefix in conversion
      return Bytes.add(Bytes.toBytes(jobId.getJobEpoch()),
                       Bytes.toBytes(jobId.getJobSequence()));
    } else {
      return Bytes.add(Bytes.toBytes(jobId.getJobPrefix()),
                       Bytes.toBytes(jobId.getJobEpoch()),
                       Bytes.toBytes(jobId.getJobSequence()));
    }
  }

  @Override
  public JobId fromBytes(byte[] bytes) {
    int packedBytesEpochSeqSize = Constants.RUN_ID_LENGTH_JOBKEY
          + Constants.SEQUENCE_NUM_LENGTH_JOBKEY;

    if (bytes == null || bytes.length < packedBytesEpochSeqSize) {
      return null;
    }

    if (bytes.length <= packedBytesEpochSeqSize) {
      // expect a packed bytes encoding of [8 bytes epoch][8 bytes seq]
      long epoch = Bytes.toLong(bytes, 0);
      long seq = Bytes.toLong(bytes, Constants.RUN_ID_LENGTH_JOBKEY);
      return new JobId(epoch, seq);
    } else {
      // expect a packed bytes encoding of [prefix][8 bytes epoch][8 bytes seq]
      String prefix = Bytes.toString(bytes, 0, bytes.length - packedBytesEpochSeqSize);
      long epoch = Bytes.toLong(bytes, bytes.length - packedBytesEpochSeqSize);
      long seq = Bytes.toLong(bytes, bytes.length - Constants.SEQUENCE_NUM_LENGTH_JOBKEY);
      return new JobId(prefix, epoch, seq);
    }
  }
}
