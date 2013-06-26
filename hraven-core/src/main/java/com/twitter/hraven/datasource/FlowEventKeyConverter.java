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
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.util.ByteUtil;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class FlowEventKeyConverter implements ByteConverter<FlowEventKey> {
  private FlowKeyConverter flowKeyConverter = new FlowKeyConverter();

  @Override
  public byte[] toBytes(FlowEventKey key) {
    if (key == null) {
      return Constants.EMPTY_BYTES;
    }
    return ByteUtil.join(Constants.SEP_BYTES, flowKeyConverter.toBytes(key),
        Bytes.toBytes(key.getSequence()));
  }

  @Override
  public FlowEventKey fromBytes(byte[] bytes) {
    byte[][] splits = ByteUtil.split(bytes, Constants.SEP_BYTES, 4);
    byte[][] flowKeySplits = new byte[4][];
    for (int i=0; i<splits.length; i++) {
      flowKeySplits[i] = splits[i];
    }
    int sequence = 0;
    if (splits.length == 4) {
      int offset = 0;
      // runId should be 8 bytes
      flowKeySplits[3] = ByteUtil.safeCopy(splits[3], offset, 8);
      // runId should be followed by SEP + sequence
      offset += 8 + Constants.SEP_BYTES.length;
      byte[] seqBytes = ByteUtil.safeCopy(splits[3], offset, 4);
      if (seqBytes != null) {
        sequence = Bytes.toInt(seqBytes);
      }
    }
    return new FlowEventKey(flowKeyConverter.fromBytes(flowKeySplits), sequence);
  }
}
