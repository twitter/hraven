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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.twitter.hraven.Range;
import com.twitter.hraven.util.BatchUtil;

/**
 *Test for the JobFilePartitioner class.
 *
 */
public class TestBatchUtil {

  /**
   * Test {@link BatchUtil#shouldRetain(int, int, int)} method.
   */
  @Test
  public void testShouldRetain() {
    assertTrue(BatchUtil.shouldRetain(0, 1, 1));
    assertTrue(BatchUtil.shouldRetain(5, 5, 10));
    
    assertFalse(BatchUtil.shouldRetain(0, 1, 2));
    assertFalse(BatchUtil.shouldRetain(4, 5, 10));
    assertFalse(BatchUtil.shouldRetain(4, 100000, 155690)); 
  }
  
  /**
   * Test {@link BatchUtil#getBatchCount(int, int)} method.
   */
  @Test
  public void testGetBatchCount() {
    // Edge cases
    assertEquals(0, BatchUtil.getBatchCount(0,0));
    assertEquals(0, BatchUtil.getBatchCount(-1,0));
    assertEquals(0, BatchUtil.getBatchCount(-2,4));
    assertEquals(0, BatchUtil.getBatchCount(5,-7));


    // One
    assertEquals(1, BatchUtil.getBatchCount(9,9));
    assertEquals(1, BatchUtil.getBatchCount(9,10));
    assertEquals(1, BatchUtil.getBatchCount(9,11));
    assertEquals(1, BatchUtil.getBatchCount(9,18));
    assertEquals(1, BatchUtil.getBatchCount(9,19));
    
    // More
    assertEquals(2, BatchUtil.getBatchCount(9,8));
    assertEquals(2, BatchUtil.getBatchCount(9,7));
    assertEquals(2, BatchUtil.getBatchCount(9,6));
    assertEquals(2, BatchUtil.getBatchCount(9,5));
    assertEquals(3, BatchUtil.getBatchCount(9,4));
    assertEquals(3, BatchUtil.getBatchCount(9,3));
    assertEquals(5, BatchUtil.getBatchCount(9,2));
    assertEquals(9, BatchUtil.getBatchCount(9,1));
  }
  
  /**
   * Confirm that getting ranges works correctly.
   */
  @Test
  public void testGetRanges() {

    List<Integer> list = Arrays.asList(1,2,3);
    List<Range<Integer>> rangeList = BatchUtil.getRanges(list, 1);
    assertEquals(3, rangeList.size());
    assertEquals(Integer.valueOf(1), rangeList.get(0).getMin());
    assertEquals(Integer.valueOf(1), rangeList.get(0).getMax());
    assertEquals(Integer.valueOf(2), rangeList.get(1).getMin());
    assertEquals(Integer.valueOf(2), rangeList.get(1).getMax());
    assertEquals(Integer.valueOf(3), rangeList.get(2).getMin());
    assertEquals(Integer.valueOf(3), rangeList.get(2).getMax());
    
    
    list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
    rangeList = BatchUtil.getRanges(list, 3);
    assertEquals(4, rangeList.size());
    assertEquals(Integer.valueOf(1), rangeList.get(0).getMin());
    assertEquals(Integer.valueOf(3), rangeList.get(0).getMax());
    assertEquals(Integer.valueOf(4), rangeList.get(1).getMin());
    assertEquals(Integer.valueOf(6), rangeList.get(1).getMax());
    assertEquals(Integer.valueOf(7), rangeList.get(2).getMin());
    assertEquals(Integer.valueOf(9), rangeList.get(2).getMax());
    assertEquals(Integer.valueOf(10), rangeList.get(3).getMin());
    assertEquals(Integer.valueOf(10), rangeList.get(3).getMax());
    
    rangeList = BatchUtil.getRanges(list, 17);
    assertEquals(1, rangeList.size());
    assertEquals(Integer.valueOf(1), rangeList.get(0).getMin());
    assertEquals(Integer.valueOf(10), rangeList.get(0).getMax());
  }

}
