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
package com.twitter.hraven.etl;

import org.apache.hadoop.hbase.util.Bytes;
import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.ByteConverter;
import com.twitter.hraven.etl.ProcessRecordKey;
import com.twitter.hraven.util.ByteUtil;

/**
 */
public class ProcessRecordKeyConverter implements ByteConverter<ProcessRecordKey> {
  @Override
  public byte[] toBytes(ProcessRecordKey key) {
    long invertedTimestamp = Long.MAX_VALUE - key.getTimestamp();
    return ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(key.getCluster()),
        Bytes.toBytes(invertedTimestamp));
  }

  @Override
  public ProcessRecordKey fromBytes(byte[] bytes) {
    byte[][] parts = ByteUtil.split(bytes, Constants.SEP_BYTES, 2);
    long invertedTimestamp = Bytes.toLong(parts[1]);
    return new ProcessRecordKey(Bytes.toString(parts[0]),
        Long.MAX_VALUE - invertedTimestamp);
  }
}
