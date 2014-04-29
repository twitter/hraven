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

package com.twitter.hraven;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCapacityDetails {

  private static final String fileNamePrefix = "/tmp/hraven-testfairscheduler-";
  private String fileName;

  @Before
  public void setUp() throws Exception {
    File srcFile = new File("src/test/resources/test-fairscheduler.xml");
    this.fileName = fileNamePrefix + System.currentTimeMillis() + ".xml";
    FileUtils.copyFile(srcFile, new File(this.fileName));
  }

  @Test
  public void testLoadCapacityDetailsFromFairScheduler() {

    Map<String, CapacityDetails> capacityInfo = new HashMap<String, CapacityDetails> ();
    Map<String, CapacityDetails> info = CapacityDetails.loadCapacityDetailsFromFairScheduler(
      "file://" + this.fileName, capacityInfo);
    assertNotNull(info);
    assertEquals(2, info.size());
    assertTrue(info.containsKey("hraven-testResources"));
    assertTrue(info.containsKey("hraven-testMapsReduces"));
    assertEquals(700000, info.get("hraven-testResources").getMinResources());
    assertEquals(0L, info.get("hraven-testResources").getMinMaps());
    assertEquals(0L, info.get("hraven-testResources").getMinReduces());
    assertEquals(0L, info.get("hraven-testMapsReduces").getMinResources());
    assertEquals(200L, info.get("hraven-testMapsReduces").getMinMaps());
    assertEquals(200L, info.get("hraven-testMapsReduces").getMinReduces());
  }

  @Test
  public void testNonExistentFairScheduler() {
    Map<String, CapacityDetails> capacityInfo = new HashMap<String, CapacityDetails> ();
    Map<String, CapacityDetails> info = CapacityDetails.loadCapacityDetailsFromFairScheduler(
      "file:///nonexistenthravenfairscheduler.xml", capacityInfo);
    assertNotNull(info);
    assertEquals(0, info.size());
  }

  @Test
  public void testIncorrectURLFairScheduler() {
    Map<String, CapacityDetails> capacityInfo = new HashMap<String, CapacityDetails> ();
    Map<String, CapacityDetails> info = CapacityDetails.loadCapacityDetailsFromFairScheduler(
      "incorrect_url_hravenfairscheduler.xml", capacityInfo);
    assertNotNull(info);
    assertEquals(0, info.size());
  }

  @After
  public void tearDownAfterTests() throws Exception {
    FileUtils.deleteQuietly(new File(this.fileName));
  }

}