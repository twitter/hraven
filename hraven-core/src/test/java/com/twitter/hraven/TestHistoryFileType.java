/*
Copyright 2013 Twitter, Inc.

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

package com.twitter.hraven;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * test class for hadoop versions
 */
public class TestHistoryFileType {

  private enum ExpHistoryFileType {
    ONE, TWO, SPARK
  }

  @Test
  public void checkVersions() {
    assertEquals(ExpHistoryFileType.values().length, HistoryFileType.values().length);
    for (HistoryFileType hv : HistoryFileType.values()) {
      assertTrue(ExpHistoryFileType.valueOf(hv.toString()) != null);
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNonExistentVersion() {
    assertNull(HistoryFileType.valueOf("DOES NOT EXIST"));
  }
};
