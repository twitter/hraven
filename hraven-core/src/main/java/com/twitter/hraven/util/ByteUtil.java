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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;

/**
 */
public class ByteUtil {

  private static final Log LOG = LogFactory.getLog(ByteUtil.class);

  public static class Range {
    private int startIdx;
    private int endIdx;

    /**
     * Defines a range from start index (inclusive) to end index (exclusive).
     * 
     * @param start
     *          Starting index position
     * @param end
     *          Ending index position (exclusive)
     */
    public Range(int start, int end) {
      if (start < 0 || end < start) {
        throw new IllegalArgumentException(
            "Invalid range, required that: 0 <= start <= end; start=" + start
                + ", end=" + end);
      }

      this.startIdx = start;
      this.endIdx = end;
    }

    public int start() {
      return startIdx;
    }

    public int end() {
      return endIdx;
    }

    public int length() {
      return endIdx - startIdx;
    }
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see
   * {@link ByteUtil#splitRanges(byte[], byte[])}.
   * 
   * @param source
   * @param separator
   * @return
   */
  public static byte[][] split(byte[] source, byte[] separator) {
    return split(source, separator, -1);
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments. To identify the split
   * ranges without the array copies, see
   * {@link ByteUtil#splitRanges(byte[], byte[])}.
   *
   * @param source
   * @param separator
   * @return
   */
  public static byte[][] split(byte[] source, byte[] separator, int limit) {
    List<Range> segments = splitRanges(source, separator, limit);

    byte[][] splits = new byte[segments.size()][];
    for (int i = 0; i < segments.size(); i++) {
      Range r = segments.get(i);
      byte[] tmp = new byte[r.length()];
      if (tmp.length > 0) {
        System.arraycopy(source, r.start(), tmp, 0, r.length());
      }
      splits[i] = tmp;
    }
    return splits;
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   */
  public static List<Range> splitRanges(byte[] source, byte[] separator) {
    return splitRanges(source, separator, -1);
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   * @param source the source data
   * @param separator the separator pattern to look for
   * @param limit the maximum number of splits to identify in the source
   */
  public static List<Range> splitRanges(byte[] source, byte[] separator, int limit) {
    List<Range> segments = new ArrayList<Range>();
    int start = 0;
    itersource: for (int i = 0; i < source.length; i++) {
      for (int j = 0; j < separator.length; j++) {
        if (source[i + j] != separator[j]) {
          continue itersource;
        }
      }
      // all separator elements matched
      if (limit > 0 && segments.size() >= (limit-1)) {
        // everything else goes in one final segment
        break;
      }

      segments.add(new Range(start, i));
      start = i + separator.length;
      // i will be incremented again in outer for loop
      i += separator.length-1;
    }
    // add in remaining to a final range
    if (start <= source.length) {
      segments.add(new Range(start, source.length));
    }
    return segments;
  }

  /**
   * Returns a single byte array containing all of the individual component
   * arrays separated by the separator array.
   * @param separator
   * @param components
   * @return
   */
  public static byte[] join(byte[] separator, byte[]... components) {
    if (components == null || components.length == 0) {
      return Constants.EMPTY_BYTES;
    }

    int finalSize = 0;
    if (separator != null) {
      finalSize = separator.length * (components.length - 1);
    }
    for (byte[] comp : components) {
      finalSize += comp.length;
    }

    byte[] buf = new byte[finalSize];
    int offset = 0;
    for (int i=0; i < components.length; i++) {
      System.arraycopy(components[i], 0, buf, offset, components[i].length);
      offset += components[i].length;
      if (i < (components.length-1) && separator != null && separator.length > 0) {
        System.arraycopy(separator, 0, buf, offset, separator.length);
        offset += separator.length;
      }
    }
    return buf;
  }

  /**
   * Returns the index (start position) of the first occurrence of the specified
   * {@code target} within {@code array} starting at {@code fromIndex} , or
   * {@code -1} if there is no such occurrence.
   * 
   * <p>
   * Returns the lowest index {@code k} such that {@code k >= fromIndex} and
   * {@code java.util.Arrays.copyOfRange(array, k, k + target.length)} contains
   * exactly the same elements as {@code target}.
   * 
   * @param array
   *          the array to search for the sequence {@code target}
   * @param target
   *          the array to search for as a sub-sequence of {@code array}
   * @param fromIndex
   *          the index to start the search from in {@code array}
   */
  public static int indexOf(byte[] array, byte[] target, int fromIndex) {

    if (array == null || target == null) {
      return -1;
    }

    // Target cannot be beyond array boundaries
    if (fromIndex < 0 || (fromIndex > (array.length - target.length))) {
      return -1;
    }

    // Empty is assumed to be at the fromIndex of any non-null array (after
    // boundary check)
    if (target.length == 0) {
      return fromIndex;
    }

    firstbyte: for (int i = fromIndex; i < array.length - target.length + 1; i++) {
      for (int j = 0; j < target.length; j++) {
        if (array[i + j] != target[j]) {
          continue firstbyte;
        }
      }
      return i;
    }
    return -1;
  }

  /**
   * Returns a copy of the source byte array, starting at offset for the given
   * length.  If the offset + length is out of bounds for the array, returns null.
   * @param source
   * @param offset
   * @param length
   * @return
   */
  public static byte[] safeCopy(byte[] source, int offset, int length) {
    if (length < 0 || source.length < offset+length) {
      return null;
    }
    byte[] copy = new byte[length];
    System.arraycopy(source, offset, copy, 0, length);
    return copy;
  }

  /**
   * return a value from the NavigableMap as a long
   * @param key
   * @param taskValues
   * @return value as Long or 0L
   */
  public static long getValueAsLong(final byte[] key,
      final Map<byte[], byte[]> taskValues) {
    byte[] value = taskValues.get(key);
    if (value != null) {
      try {
      long retValue = Bytes.toLong(value);
      return retValue;
      } catch (NumberFormatException nfe) {
        LOG.error("Caught NFE while converting to long ", nfe);
        return 0L;
      } catch (IllegalArgumentException iae ) {
        // for exceptions like java.lang.IllegalArgumentException:
        // offset (0) + length (8) exceed the capacity of the array: 7
        LOG.error("Caught IAE while converting to long ", iae);
        return 0L;
      }
    } else {
      return 0L;
    }
  }

  /**
   * return a value from the NavigableMap as a String
   * @param key
   * @param taskValues
   * @return value as a String or ""
   */
  public static String getValueAsString(final byte[] key,
      final Map<byte[], byte[]> taskValues) {
    byte[] value = taskValues.get(key);
    if (value != null) {
      return Bytes.toString(value);
    } else {
      return "";
    }
  }

  /**
   * return a value from the NavigableMap as a Double
   * @param key to be looked up for the value
   * @param infoValues - the map containing the key values
   * @return value as Double or 0.0
   */
  public static double getValueAsDouble(byte[] key,
      NavigableMap<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(key);
    if (value != null) {
      return Bytes.toDouble(value);
    } else {
      return 0.0;
    }
  }

  /**
   * get value from a map as an int
   * @param key
   * @param infoValues
   * @return int
   */
  public static int getValueAsInt(byte[] key, Map<byte[], byte[]> infoValues) {
    byte[] value = infoValues.get(key);
    if (value != null) {
      try {
        int retValue = Bytes.toInt(value);
        return retValue;
      } catch (NumberFormatException nfe) {
        LOG.error("Caught NFE while converting to int ", nfe);
        return 0;
      } catch (IllegalArgumentException iae) {
        // for exceptions like java.lang.IllegalArgumentException:
        // offset (0) + length (8) exceed the capacity of the array: 7
        LOG.error("Caught IAE while converting to int ", iae);
        return 0;
      }
    } else {
      return 0;
    }
  }

}
