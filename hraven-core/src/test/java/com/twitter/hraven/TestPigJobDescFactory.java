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

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.twitter.hraven.PigJobDescFactory;

public class TestPigJobDescFactory {

  String[][] testJobNames = {
    { null, null },
    { "foo", "foo" },
    { "PigLatin:daily_job:daily_2012/06/22-00:00:00_to_2012/06/23-00:00:00",
        PigJobDescFactory.SCHEDULED_PREFIX + "daily_job:daily" },
    { "PigLatin:hourly_job:hourly_2012/06/24-08:00:00_to_2012/06/24-09:00:00",
        PigJobDescFactory.SCHEDULED_PREFIX + "hourly_job:hourly" },
    { "PigLatin:hourly_foo:hourly:foo_2012/06/24-08:00:00_to_2012/06/24-09:00:00",
        PigJobDescFactory.SCHEDULED_PREFIX + "hourly_foo:hourly:foo" },
    { "PigLatin:regular_job.pig", "PigLatin:regular_job.pig" }
  };

  Object[][] testLogFileNames = {
    { null, 0L },
    { "/var/log/pig/pig_1340659035863.log", 1340659035863L },
    { "/var/log/pig/pig_log.log", 0L },
  };

  @Test
  public void testJobNameToBatchDesc() {
    PigJobDescFactory pigFactory = new PigJobDescFactory();
    for (String[] inputOuput : testJobNames) {
      String input = inputOuput[0];
      String expected = inputOuput[1];

      String found = pigFactory.getAppIdFromJobName(input);
      assertEquals("Unexpected result found when parsing jobName=" + input, expected, found);
    }
  }

  @Test
  public void testLogFileToStartTime() {

    for (Object[] inputOuput : testLogFileNames) {
      String input = (String)inputOuput[0];
      long expected = (Long)inputOuput[1];

      long found = PigJobDescFactory.getScriptStartTimeFromLogfileName(input);
      assertEquals("Unexpected result found when parsing logFileName=" + input, expected, found);
    }
  }
}
