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

package com.twitter.hraven.datasource;

import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.AppAggregationKey;
import com.twitter.hraven.Constants;
import com.twitter.hraven.util.ByteUtil;

/**
 * To convert the row key into {@link AppAggregationKey} components
 * and vice versa
 *
 */
public class AppAggregationKeyConverter implements ByteConverter<AppAggregationKey> {

  @Override
  public byte[] toBytes(AppAggregationKey appAggKey) {
    if (appAggKey == null) {
      return Constants.EMPTY_BYTES;
    } else {
      return ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes(appAggKey.getCluster()),
          Bytes.toBytes(appAggKey.getEncodedRunId()),
          Bytes.toBytes(appAggKey.getUserName()),
          Bytes.toBytes(appAggKey.getAppId()));
    }
  }

  @Override
  public AppAggregationKey fromBytes(byte[] bytes) {
    return fromBytes(ByteUtil.split(bytes, Constants.SEP_BYTES, 4));
  }

  public AppAggregationKey fromBytes(byte[][] splitBytes) {
    long runId = splitBytes.length > 1 ? Long.MAX_VALUE - Bytes.toLong(splitBytes[1]) : 0;
    return new AppAggregationKey( Bytes.toString(splitBytes[0]),
        splitBytes.length > 2 ? Bytes.toString(splitBytes[2]) : null,
        splitBytes.length > 3 ? Bytes.toString(splitBytes[3]) : null,
        runId);
  }
}
