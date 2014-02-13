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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.QualifiedPathKey;
import com.twitter.hraven.util.ByteUtil;

/**
 * Converter is needed to interpret the elements of
 * an hdfs stats key
 * based on the byte representation of the row key
 */
public class HdfsStatsKeyConverter implements ByteConverter<HdfsStatsKey> {

  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(HdfsStatsKeyConverter.class);

  /**
   * Returns the byte encoded representation of a HdfsStatsKey
   *
   * @param hdfsStats the HdfsStatsKey to serialize
   * @return the byte encoded representation of the HdfsStatsKey
   */
  @Override
  public byte[] toBytes(HdfsStatsKey hdfsStatsKey) {
    if (hdfsStatsKey == null || hdfsStatsKey.getQualifiedPathKey() == null) {
      return HdfsConstants.EMPTY_BYTES;
    } else {
      return ByteUtil.join(HdfsConstants.SEP_BYTES,
          Bytes.toBytes(Long.toString(hdfsStatsKey.getEncodedRunId())),
          Bytes.toBytes(hdfsStatsKey.getQualifiedPathKey().getCluster()),
          Bytes.toBytes(hdfsStatsKey.getQualifiedPathKey().getPath()));
    }
  }

  /**}
   * Converts HdfsStatsKeys from its byte encoded representation
   * @param bytes the serialized version of a HdfsStatsKey
   * @return a deserialized HdfsStatsKey instance
   */
  public HdfsStatsKey fromBytes(byte[] bytes) {
    byte[][] splits = splitHdfsStatsKey(bytes);
    return parseHdfsStatsKey(splits);
  }

  /**
   * Constructs a HdfsStatsKey instance from the
   * individual byte encoded key components.
   *
   * @param keyComponents
   *          as split on Separator '!'
   * @return a HdfsStatsKey instance containing the decoded components
   */
  public static HdfsStatsKey parseHdfsStatsKey(byte[][] keyComponents) {
    return new HdfsStatsKey(new QualifiedPathKey(
        // cluster:
        (keyComponents.length > 1 ? Bytes.toString(keyComponents[1]) : null),
        // path:
        (keyComponents.length > 2 ? Bytes.toString(keyComponents[2]) : null)),
        // encodedRunId:
        (keyComponents.length > 0 ? Long.parseLong(Bytes.toString(keyComponents[0])) : null));
  }

  /**
   * Handles splitting the encoded hdfsStats key
   *
   * @param rawKey byte encoded representation of the path key
   * @return
   */
  static byte[][] splitHdfsStatsKey(byte[] rawKey) {
    byte[][] splits = ByteUtil.split(rawKey, HdfsConstants.SEP_BYTES,
          HdfsConstants.NUM_HDFS_USAGE_ROWKEY_COMPONENTS);
    return splits;
  }
}
