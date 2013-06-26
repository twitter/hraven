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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.util.ByteUtil;

/**
 * Match up to N runs of a given app.  Once N runs have been seen, we filter all
 * remaining rows.
 */
public class RunMatchFilter extends FilterBase {
  private byte[] appId;
  private int maxCount;
  private byte[] lastRunId = null;
  private int seenCount;

  /**
   * Match only a single run of the given appId
   * @param appId
   */
  public RunMatchFilter(String appId) {
    this(appId, 1);
  }

  /**
   * Match up to maxCount runs of the given appId
   *
   * @param appId
   * @param maxCount
   */
  public RunMatchFilter(String appId, int maxCount) {
    this.appId = Bytes.toBytes(appId);
    this.maxCount = maxCount;
  }

  @Override
  public void reset() {
    this.seenCount = 0;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    // TODO: don't copy the byte[]
    byte[] rowkey = new byte[length];
    System.arraycopy(buffer, offset, rowkey, 0, length);
    List<ByteUtil.Range> splits = ByteUtil.splitRanges(rowkey, Constants.SEP_BYTES);
    if (splits.size() < 4) {
      // invalid row key
      return true;
    }
    ByteUtil.Range appRange = splits.get(1);
    int appCompare = Bytes.compareTo(appId, 0, appId.length,
        rowkey, appRange.start(), appRange.length());
    if (appCompare != 0) {
      return false;
    }
    ByteUtil.Range runRange = splits.get(2);
    int runLength = runRange.length();
    if (lastRunId == null ||
        Bytes.compareTo(lastRunId, 0, lastRunId.length,
            rowkey, runRange.start(), runLength) != 0) {
      lastRunId = new byte[runLength];
      System.arraycopy(rowkey, runRange.start(), lastRunId, 0, runLength);
      seenCount++;
    }

    return seenCount > maxCount;
  }

  @Override
  public boolean filterAllRemaining() {
    // once we've seen the limit number of runs, skip everything else
    return seenCount > maxCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(appId.length);
    out.write(appId);
    out.writeInt(maxCount);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int appIdLength = in.readInt();
    this.appId = new byte[appIdLength];
    in.readFully(appId);
    this.maxCount = in.readInt();
  }
}
