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

import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.FlowQueueKey;
import com.twitter.hraven.util.ByteUtil;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Handles serialization and deserialization of a {@link FlowQueueKey} to and from bytes.
 */
public class FlowQueueKeyConverter implements ByteConverter<FlowQueueKey> {
  @Override
  public byte[] toBytes(FlowQueueKey key) {
    if (key == null) {
      return Constants.EMPTY_BYTES;
    }
    long invertedTimestamp = Long.MAX_VALUE - key.getTimestamp();
    return ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(key.getCluster()),
        (key.getStatus() == null ? Constants.EMPTY_BYTES : key.getStatus().code()),
        Bytes.toBytes(invertedTimestamp),
        Bytes.toBytes(key.getFlowId()));
  }

  @Override
  public FlowQueueKey fromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    byte[][] firstSplit = ByteUtil.split(bytes, Constants.SEP_BYTES, 3);
    byte[] timestampBytes = null;
    byte[] flowIdBytes = null;
    if (firstSplit.length == 3) {
      int offset = 0;
      timestampBytes = ByteUtil.safeCopy(firstSplit[2], 0, 8);
      offset += 8+Constants.SEP_BYTES.length;
      flowIdBytes = ByteUtil.safeCopy(firstSplit[2], offset, firstSplit[2].length - offset);
    }

    return new FlowQueueKey(Bytes.toString(firstSplit[0]),
        firstSplit.length > 1 ? Flow.STATUS_BY_CODE.get(firstSplit[1]) : null,
        timestampBytes != null ? Long.MAX_VALUE - Bytes.toLong(timestampBytes) : 0,
        flowIdBytes != null ? Bytes.toString(flowIdBytes) : null);
  }
}
