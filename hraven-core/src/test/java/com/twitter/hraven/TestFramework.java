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
package com.twitter.hraven;

import static com.twitter.hraven.Framework.NONE;
import static com.twitter.hraven.Framework.PIG;
import static com.twitter.hraven.Framework.SCALDING;
import static com.twitter.hraven.Framework.SPARK;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import org.junit.Test;

import com.twitter.hraven.Framework;

/**
 * Test {@link Framework}
 */
public class TestFramework {

  /**
   * Test going back and forth between code and enum
   */
  @Test
  public void testGetCode() {
    assertEquals(PIG, Framework.get(PIG.getCode()));
    assertEquals(SCALDING, Framework.get(SCALDING.getCode()));
    assertEquals(NONE, Framework.get(NONE.getCode()));
    assertEquals(SPARK,Framework.get(SPARK.getCode()));
  }

  /**
   * Confirm descriptions are not null or empty.
   */
  @Test
  public void getDescription() {
    assertNotNull(PIG.getDescription());
    assertNotNull(SCALDING.getDescription());
    assertNotNull(NONE.getDescription());
    assertNotNull(SPARK.getDescription());
    assertTrue("Description is not expected to be empty", PIG.getDescription().length() > 0);
    assertTrue("Description is not expected to be empty", SCALDING.getDescription().length() > 0);
    assertTrue("Description is not expected to be empty", NONE.getDescription().length() > 0);
    assertTrue("Description is not expected to be empty", SPARK.getDescription().length() > 0);
  }
  
  
}
