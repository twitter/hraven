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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.twitter.hraven.Range;

/**
 * Utility class that helps process items in batches.
 */
public class BatchUtil {

  /**
   * Method that can be used when iterating over an array and you want to retain
   * only maxRetention items.
   * 
   * @param i
   *          index of element in ordered array of length
   * @param maxRetention
   *          total number of elements to retain.
   * @param length
   *          of the ordered array
   * @return whether this element should be retained or not.
   */
  public static boolean shouldRetain(int i, int maxRetention, int length) {
    // Files with a zero-based index greater or equal than the retentionCutoff
    // should be retained.
    int retentionCutoff = length - maxRetention;
    boolean retain = (i >= retentionCutoff) ? true : false;
    return retain;
  }

  /**
   * @param length
   *          of the items to process
   * @param batchSize
   *          size of the batch.
   * @return return the number of batches it takes to process length items in
   *         batchSize batches.
   */
  public static int getBatchCount(int length, int batchSize) {
    // Avoid negative numbers.
    if ((batchSize < 1) || (length < 1)) {
      return 0;
    }
    int remainder = length % batchSize;
    return (remainder > 0) ? (length / batchSize) + 1 : (length / batchSize);
  }

  /**
   * Given an (ordered) {@link Collection} of non-<code>null</code>, iterate
   * over the collection and return a list of ranges of the given batchSize. The
   * last range may be smaller and contains the remainder if the collection is
   * not equally divisible in batchSize ranges.
   * 
   * @param <E>
   *          The class of Elements in the collection out of which to create
   *          ranges.
   * 
   * @param collection
   *          of non-<code>null</code> elements to chop up in ranges.
   * @param batchSize
   *          the size to chop the collection into. Must be larger than
   *          <code>1</code> and can be larger than the collection.
   * @return a non-null list of ranges.
   *         <p>
   *         For example, <code>getRanges([1,2,3], 1)</code> is
   *         <code>[1-1,2-2,3-3]</code> and
   *         <code>getRanges([1,2,3,4,5,6,7,8,9,10], 3)</code> is
   *         <code>[1-3,4-6,7-9,10-10]</code> and
   *         <code>getRanges([1,2,3,4,5,6,7,8,9,10], 17)</code> is
   *         <code>[1-10]</code>
   */
  public static <E extends Comparable<E>> List<Range<E>> getRanges(Collection<E> collection,
      int batchSize) {

    List<Range<E>> rangeList = new LinkedList<Range<E>>();

    E currentMin = null;

    // Check for edge cases
    if ((collection != null) && (collection.size() > 0) && (batchSize > 0)) {

      int index = 1;
      for (E element : collection) {
        // Bootstrap first element in the next range
        if (currentMin == null) {
          currentMin = element;
        }

        int mod = index % batchSize;
        // On each batchSize items (and the last one) create a range
        if ((mod == 0) || (index == collection.size())) {
          Range<E> range = new Range<E>(currentMin, element);
          rangeList.add(range);
          currentMin = null;
        }

        index++;
      }
    }

    return rangeList;
  }
}
