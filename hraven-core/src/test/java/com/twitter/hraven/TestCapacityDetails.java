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
import static org.junit.Assert.assertNull;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.hraven.datasource.ProcessingException;

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
    CapacityDetails cd =
        new CapacityDetails(SchedulerTypes.FAIR_SCHEDULER.toString(), "file://" + this.fileName);
    assertNotNull(cd);
    assertEquals(2, cd.size());
    // test for min resources for pool hraven-testResources
    // and min maps, reduces to be  0
    assertEquals(700000, cd.getMinResources("hraven-testResources"));
    assertEquals(0L, cd.getMinMaps("hraven-testResources"));
    assertEquals(0L, cd.getMinReduces("hraven-testResources"));
    assertEquals(0L, cd.getMinResources("hraven-testMapsReduces"));
    assertEquals(200L, cd.getMinMaps("hraven-testMapsReduces"));
    assertEquals(200L, cd.getMinReduces("hraven-testMapsReduces"));
    // test for non existent queue
    assertEquals(0L, cd.getMinMaps("something"));
  }

  @Test
  public void testNonExistentFairScheduler() {
    CapacityDetails cd = new CapacityDetails(
            SchedulerTypes.FAIR_SCHEDULER.toString(),
            "file:///nonexistenthravenfairscheduler.xml");
    assertNotNull(cd);
    assertEquals(0, cd.size());
  }

  @Test(expected=ProcessingException.class)
  public void testNonExistentSchedulerType() {
    CapacityDetails cd = new CapacityDetails(
            "abcd",
            "file://" + this.fileName);
    assertNull(cd);
  }

  @Test(expected=ProcessingException.class)
  public void testNullSchedulerType() {
    CapacityDetails cd = new CapacityDetails(
            null,
            "file://" + this.fileName);
    assertNull(cd);
  }

  @Test
  public void testIncorrectURLFairScheduler() {
    CapacityDetails cd = new CapacityDetails(
            SchedulerTypes.FAIR_SCHEDULER.toString(),
            "incorrect_url_hravenfairscheduler.xml");
    assertNotNull(cd);
    assertEquals(0, cd.size());
  }

  @After
  public void tearDownAfterTests() throws Exception {
    FileUtils.deleteQuietly(new File(this.fileName));
  }

}