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
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.util.ByteUtil;

import org.apache.hadoop.hbase.util.Bytes;

/**
 */
public class FlowKeyConverter implements ByteConverter<FlowKey> {

  @Override
  public byte[] toBytes(FlowKey flowKey) {
    if (flowKey == null) {
      return Constants.EMPTY_BYTES;
    } else {
      return ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes(flowKey.getCluster()),
          Bytes.toBytes(flowKey.getUserName()),
          Bytes.toBytes(flowKey.getAppId()),
          Bytes.toBytes(flowKey.getEncodedRunId()));
    }
  }

  @Override
  public FlowKey fromBytes(byte[] bytes) {
    return fromBytes(ByteUtil.split(bytes, Constants.SEP_BYTES, 4));
  }

  public FlowKey fromBytes(byte[][] splitBytes) {
    long runId = splitBytes.length > 3 ? Long.MAX_VALUE - Bytes.toLong(splitBytes[3]) : 0;
    return new FlowKey( Bytes.toString(splitBytes[0]),
        splitBytes.length > 1 ? Bytes.toString(splitBytes[1]) : null,
        splitBytes.length > 2 ? Bytes.toString(splitBytes[2]) : null,
        runId);
  }
}
