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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.twitter.hraven.JobId;
import com.twitter.hraven.Range;
import com.twitter.hraven.util.BatchUtil;

public class TestJobHistoryRawService {


  /**
   * Does not test a specific method, but tests the algorithm used to get
   * ranges.
   */
  @Test
  public void testGetJobIdRanges() {

    long aEpoch = 123456;
    long bEpoch = 234567;

    JobId aOne = new JobId(aEpoch, 1);
    JobId aTwo = new JobId(aEpoch, 2);
    JobId aThree = new JobId(aEpoch, 3);
    JobId aSeven = new JobId(aEpoch, 7);
    
    JobId aThirteen = new JobId(aEpoch, 13);
    JobId aHundredOne = new JobId(aEpoch, 101);
    JobId bOne = new JobId(bEpoch, 1);
    JobId bTwo = new JobId(bEpoch, 2);
    
    JobId bThree = new JobId(bEpoch, 3);
    JobId bSeven = new JobId(bEpoch, 7);
    JobId bThirteen = new JobId(bEpoch, 13);
    JobId bHundredOne = new JobId(bEpoch, 101);

    SortedSet<JobId> orderedJobIds = new TreeSet<JobId>();
    // Add in scrambled order
    orderedJobIds.add(bSeven);
    orderedJobIds.add(aSeven);
    orderedJobIds.add(aThree);
    orderedJobIds.add(bThree);
    orderedJobIds.add(bThirteen);
    orderedJobIds.add(aThirteen);
    orderedJobIds.add(aOne);
    orderedJobIds.add(bOne);
    orderedJobIds.add(aHundredOne);
    orderedJobIds.add(bHundredOne);
    orderedJobIds.add(aTwo);
    orderedJobIds.add(bTwo);

    // And for good measure add these again, set should take them out.
    orderedJobIds.add(aTwo);
    orderedJobIds.add(bTwo);
    
    assertEquals(12, orderedJobIds.size());

    List<Range<JobId>> ranges = BatchUtil.getRanges(orderedJobIds, 4);
    assertEquals(3, ranges.size());
    assertEquals(1, ranges.get(0).getMin().getJobSequence());
    assertEquals(7, ranges.get(0).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(0).getMin().getJobEpoch());
    assertEquals(aEpoch, ranges.get(0).getMax().getJobEpoch());
    
    assertEquals(13, ranges.get(1).getMin().getJobSequence());
    assertEquals(2, ranges.get(1).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(1).getMin().getJobEpoch());
    assertEquals(bEpoch, ranges.get(1).getMax().getJobEpoch());
    
    assertEquals(3, ranges.get(2).getMin().getJobSequence());
    assertEquals(101, ranges.get(2).getMax().getJobSequence());
    assertEquals(bEpoch, ranges.get(2).getMin().getJobEpoch());
    assertEquals(bEpoch, ranges.get(2).getMax().getJobEpoch());
    
    long cEpoch = 345678;
    long triangular = 1000405;
    JobId cTriangular = new JobId(cEpoch, triangular);
    orderedJobIds.add(cTriangular);
    
    assertEquals(13, orderedJobIds.size());
    ranges = BatchUtil.getRanges(orderedJobIds, 4);
    assertEquals(4, ranges.size());
    assertEquals(triangular, ranges.get(3).getMin().getJobSequence());
    assertEquals(triangular, ranges.get(3).getMax().getJobSequence());
    assertEquals(cEpoch, ranges.get(3).getMin().getJobEpoch());
    assertEquals(cEpoch, ranges.get(3).getMax().getJobEpoch());
    
    ranges = BatchUtil.getRanges(orderedJobIds, 1000);
    assertEquals(1, ranges.size());
    assertEquals(1, ranges.get(0).getMin().getJobSequence());
    assertEquals(triangular, ranges.get(0).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(0).getMin().getJobEpoch());
    assertEquals(cEpoch, ranges.get(0).getMax().getJobEpoch());
  }
}
