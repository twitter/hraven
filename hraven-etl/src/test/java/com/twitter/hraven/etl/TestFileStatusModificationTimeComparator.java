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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;

import com.twitter.hraven.etl.FileStatusModificationComparator;

/**
 * Test the FileStatusModificationComparator
 */
public class TestFileStatusModificationTimeComparator {

  private static final FileStatus fileStatus1 = new FileStatus(0, false, 0, 0,
      13, null);
  private static final FileStatus fileStatus2 = new FileStatus(0, false, 0, 0,
      17, null);

  /**
   * Do the needed.
   */
  @Test
  public void testCompare() {

    FileStatusModificationComparator fsModComp = new FileStatusModificationComparator();

    assertEquals(0, fsModComp.compare(fileStatus1, fileStatus1));
    assertEquals(0, fsModComp.compare(fileStatus2, fileStatus2));
    assertEquals(0, fsModComp.compare(null, null));

    // Smaller
    assertEquals(-1, fsModComp.compare(null, fileStatus1));
    assertEquals(-1, fsModComp.compare(null, fileStatus2));
    assertEquals(-1, fsModComp.compare(fileStatus1, fileStatus2));

    // Bigger
    assertEquals(1, fsModComp.compare(fileStatus1, null));
    assertEquals(1, fsModComp.compare(fileStatus2, null));
    assertEquals(1, fsModComp.compare(fileStatus2, fileStatus1));

    int x = 10;
    int y = 3;
    int q = x / y;
    int r = x % y;
    int b =  (r > 0) ? (x / y) + 1 : (x / y);
    System.out.println("x=" + x + " y=" + y + " q=" + q + " r=" + r + " b=" + b);
    
  }

}
