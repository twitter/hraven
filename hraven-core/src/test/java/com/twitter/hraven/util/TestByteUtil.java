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
package com.twitter.hraven.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.util.ByteUtil;

/**
 */
public class TestByteUtil {
  private byte[] source1 = Bytes.toBytes("abc!bcd!cde");
  byte[] sep1 = Bytes.toBytes("!");

  byte[] source2 = Bytes.toBytes("random::stuff::");
  byte[] sep2 = Bytes.toBytes("::");

  byte[] source3 = Bytes.toBytes("::more::stuff::");
  byte[] sep3 = Bytes.toBytes("::");

  byte[] source4 = Bytes.toBytes("singlesource");
  byte[] sep4 = Bytes.toBytes("::");

  byte[] source5 = Bytes.toBytes("abc!!bcd");
  byte[] sep5 = Bytes.toBytes("!");

  byte[] source6 = Bytes.toBytes("single:source");
  byte[] sep6 = Bytes.toBytes("::");

  private static final class JobDetailsValues {
    // job-level stats
    static final String jobName = "Sleep Job";
    static final String version = "5b6900cfdcaa2a17db3d5f3f";
    static final long totalMaps = 100L;
    static final long megabytemillis = 46317568L;
  }

  @Test
  public void testSplit() {
    // single byte separator
    byte[][] expected1 = Bytes
        .toByteArrays(new String[] { "abc", "bcd", "cde" });

    byte[][] splitresult = ByteUtil.split(source1, sep1);
    assertSplitResult(expected1, splitresult);

    // multi-byte separator, plus trailing separator
    byte[][] expected2 = Bytes.toByteArrays(new String[] { "random", "stuff",
        "" });
    splitresult = ByteUtil.split(source2, sep2);
    assertSplitResult(expected2, splitresult);

    // leading and trailing separator
    byte[][] expected3 = Bytes.toByteArrays(new String[] { "", "more", "stuff",
        "" });
    splitresult = ByteUtil.split(source3, sep3);
    assertSplitResult(expected3, splitresult);

    // source with no separator
    byte[][] expected4 = Bytes.toByteArrays(new String[] { "singlesource" });
    splitresult = ByteUtil.split(source4, sep4);
    assertSplitResult(expected4, splitresult);

    // source with empty component
    byte[][] expected5 =
        Bytes.toByteArrays(new String[]{ "abc", "", "bcd"});
    splitresult = ByteUtil.split(source5, sep5);
    assertSplitResult(expected5, splitresult);

    byte[][] expected6 = new byte[][] {source6};
    splitresult = ByteUtil.split(source6, sep6);
    assertSplitResult(expected6, splitresult);
  }

  @Test
  public void testSplitWithLimit() {
    // source with more splits than the limit
    byte[][] expectedResult = Bytes.toByteArrays(new String[] {"random", "stuff::"});
    byte[][] splitResult = ByteUtil.split(source2, sep2, 2);
    assertSplitResult(expectedResult, splitResult);

    // source with fewer splits than the limit
    expectedResult = Bytes.toByteArrays(new String[] {"random", "stuff", ""});
    splitResult = ByteUtil.split(source2, sep2, 100);
    assertSplitResult(expectedResult, splitResult);

    // source with limit of 1
    expectedResult = new byte[][] {source2};
    splitResult = ByteUtil.split(source2, sep2, 1);
    assertSplitResult(expectedResult, splitResult);
  }

  private void assertSplitResult(byte[][] expectedResult, byte[][] actualResult) {
    assertEquals(expectedResult.length, actualResult.length);
    for (int i = 0; i < actualResult.length; i++) {
      assertArrayEquals("Result " + i + " should match", expectedResult[i],
          actualResult[i]);
    }
  }

  @Test
  public void testSplitRanges() {
    // test basic range sanity checking
    try {
      new ByteUtil.Range(-1, 1);
      fail("Should have failed with start < 0");
    } catch (IllegalArgumentException expected) {
    }
    try {
      new ByteUtil.Range(2, 1);
      fail("Should have failed with end < start");
    } catch (IllegalArgumentException expected) {
    }

    List<ByteUtil.Range> ranges1 = ByteUtil.splitRanges(source1, sep1);
    assertEquals("source1 should have 3 segments", 3, ranges1.size());
    assertEquals(0, ranges1.get(0).start());
    assertEquals(3, ranges1.get(0).length());
    assertEquals(4, ranges1.get(1).start());
    assertEquals(3, ranges1.get(1).length());
    assertEquals(8, ranges1.get(2).start());
    assertEquals(3, ranges1.get(2).length());

    List<ByteUtil.Range> ranges4 = ByteUtil.splitRanges(source4, sep4);
    assertEquals("source4 should be a single segment", 1, ranges4.size());
    assertEquals(0, ranges4.get(0).start());
    assertEquals(source4.length, ranges4.get(0).length());

    List<ByteUtil.Range> ranges5 = ByteUtil.splitRanges(source5, sep5);
    assertEquals(3, ranges5.size());
    assertEquals(0, ranges5.get(0).start());
    assertEquals(3, ranges5.get(0).end());
    assertEquals(4, ranges5.get(1).start());
    assertEquals(4, ranges5.get(1).end());
    assertEquals(5, ranges5.get(2).start());
    assertEquals(8, ranges5.get(2).end());
  }

  @Test
  public void testJoin() {
    byte[] comp1 = Bytes.toBytes("abc");
    byte[] comp2 = Bytes.toBytes("def");
    byte[] comp3 = Bytes.toBytes("ghi");

    // test empty case
    byte[] joined = ByteUtil.join(Constants.SEP_BYTES);
    assertNotNull(joined);
    assertEquals(0, joined.length);

    // test no separator
    joined = ByteUtil.join(null, comp1, comp2, comp3);
    assertNotNull(joined);
    assertArrayEquals(Bytes.toBytes("abcdefghi"), joined);

    // test normal case
    joined = ByteUtil.join(Constants.SEP_BYTES, comp1, comp2, comp3);
    assertNotNull(joined);
    assertArrayEquals(
        Bytes.toBytes("abc"+Constants.SEP+"def"+Constants.SEP+"ghi"), joined);
  }

  /**
   * 
   */
  @Test
  public void testIndexOf() {

    byte[] array = Bytes.toBytes("quackattack");
    byte[] a = Bytes.toBytes("a");
    byte[] ack = Bytes.toBytes("ack");
    byte[] empty = Bytes.toBytes("");

    int index = ByteUtil.indexOf(array, null, 0);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(null, ack, 0);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(null, ack, 1);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, ack, 100);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, ack, 100);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, a, array.length + 1);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, ack, array.length + 1);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, empty, array.length + 1);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, empty, 100);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, ack, -3);
    assertEquals(-1, index);

    index = ByteUtil.indexOf(array, empty, -3);
    assertEquals(-1, index);

    // Empty array should be at the startIndex
    index = ByteUtil.indexOf(array, empty, 0);
    assertEquals(0, index);

    index = ByteUtil.indexOf(array, empty, 4);
    assertEquals(4, index);

    // Normal cases
    assertIndexOf(0, array, empty, 0);
    assertIndexOf(1, array, empty, 1);
    assertIndexOf(3, array, empty, 3);
    assertIndexOf(5, array, empty, 5);
    assertIndexOf(11, array, empty, 11);

    assertIndexOf(2, array, a, 0);
    assertIndexOf(2, array, a, 1);
    assertIndexOf(2, array, a, 2);
    assertIndexOf(5, array, a, 3);
    assertIndexOf(5, array, a, 4);

    assertIndexOf(2, array, ack, 0);
    assertIndexOf(2, array, ack, 1);
    assertIndexOf(2, array, ack, 2);
    assertIndexOf(8, array, ack, 3);
    assertIndexOf(8, array, ack, 4);
    assertIndexOf(8, array, ack, 8);
  }

  /**
   * @param expectedIndex
   *          where the index is expected to be
   * @param array
   *          to search through
   * @param target
   *          to search for
   * @param fromIndex
   *          to start search from
   */
  private static void assertIndexOf(int expectedIndex, byte[] array,
      byte[] target, int fromIndex) {
    int index = ByteUtil.indexOf(array, target, fromIndex);
    assertEquals(expectedIndex, index);
    byte[] sub = java.util.Arrays.copyOfRange(array, index, index
        + target.length);
    assertEquals(0, Bytes.compareTo(target, sub));
  }

  /**
   * test get value as long
   */
  @Test
  public void testGetValueAsLong() {
    NavigableMap<byte[], byte[]> infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_MAPS),
      Bytes.toBytes(JobDetailsValues.totalMaps));
    Long expVal = JobDetailsValues.totalMaps;
    assertEquals(expVal, ByteUtil.getValueAsLong(JobHistoryKeys.KEYS_TO_BYTES
      .get(JobHistoryKeys.TOTAL_MAPS), infoValues));

    // test non existent value
    expVal = 0L;
    assertEquals(expVal, ByteUtil.getValueAsLong(JobHistoryKeys.KEYS_TO_BYTES
      .get(JobHistoryKeys.TOTAL_REDUCES), infoValues));
    
    infoValues.put(Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(JobDetailsValues.megabytemillis));
    expVal = JobDetailsValues.megabytemillis;
    assertEquals(expVal, ByteUtil.getValueAsLong(Constants.MEGABYTEMILLIS_BYTES, infoValues));

    // test non existent value
    expVal = 0L;
    assertEquals(expVal, ByteUtil.getValueAsLong(Constants.HRAVEN_QUEUE_BYTES, infoValues));

    infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    infoValues.put(Bytes.toBytes("checking_iae"),
      Bytes.toBytes("abc"));
    assertEquals(expVal, ByteUtil.getValueAsLong(Bytes.toBytes("checking_iae"), infoValues));

  }

  /**
   * test get value as String
   */
  @Test
  public void testGetValueAsString() {
    NavigableMap<byte[], byte[]> infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    infoValues.put(JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBNAME),
      Bytes.toBytes(JobDetailsValues.jobName));
    assertEquals(JobDetailsValues.jobName, ByteUtil.getValueAsString(
      JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBNAME), infoValues));
    // test non existent values
    assertEquals("", ByteUtil.getValueAsString(
      JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_QUEUE), infoValues));
  }

  /**
   * test get value as String
   */
  @Test
  public void testGetValueAsString2() {
    NavigableMap<byte[], byte[]> infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    infoValues.put(Constants.VERSION_COLUMN_BYTES, Bytes.toBytes(JobDetailsValues.version));
    assertEquals(JobDetailsValues.version,
      ByteUtil.getValueAsString(Constants.VERSION_COLUMN_BYTES, infoValues));
    // test non existent values
    assertEquals("", ByteUtil.getValueAsString(Constants.HRAVEN_QUEUE_BYTES, infoValues));
  }

  /**
   * test get value as long
   */
  @Test
  public void testGetValueAsDouble() {
    NavigableMap<byte[], byte[]> infoValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    double value = 34.567;
    double delta = 0.01;
    byte[] key = Bytes.toBytes("testingForDouble");
    infoValues.put(key, Bytes.toBytes(value));
    assertEquals(value, ByteUtil.getValueAsDouble(key, infoValues), delta);

    // test non existent value
    double expVal = 0.0;
    key = Bytes.toBytes("testingForDoubleNonExistent");
    assertEquals(expVal, ByteUtil.getValueAsDouble(key, infoValues), delta);

  }

}
