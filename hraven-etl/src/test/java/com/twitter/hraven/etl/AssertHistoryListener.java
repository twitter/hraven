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
package com.twitter.hraven.etl;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobHistoryCopy;

import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.Counter;
import com.twitter.hraven.CounterMap;
import com.twitter.hraven.JobDetails;

/**
 * 
 */
public class AssertHistoryListener implements JobHistoryCopy.Listener {
 private Log log = LogFactory.getLog(getClass());

  private JobDetails jobDetails;
  private boolean assertedCounters = false;
  private boolean assertedMapCounters = false;
  private boolean assertedReduceCounters = false;

  private Set<Pair<JobHistoryKeys, String>> keysToAssert =
    new HashSet<Pair<JobHistoryKeys, String>>();

  // Store status so we can assert against final value at the end
  private String status = null;

  public AssertHistoryListener(JobDetails jobDetails) {
    this.jobDetails = jobDetails;

    addExpectedKey(JobHistoryKeys.TOTAL_MAPS, jobDetails.getTotalMaps());
    addExpectedKey(JobHistoryKeys.TOTAL_REDUCES, jobDetails.getTotalReduces());
    addExpectedKey(JobHistoryKeys.FAILED_MAPS, jobDetails.getFailedMaps());
    addExpectedKey(JobHistoryKeys.FAILED_REDUCES, jobDetails.getFailedReduces());
    addExpectedKey(JobHistoryKeys.FINISH_TIME, jobDetails.getFinishTime());
    addExpectedKey(JobHistoryKeys.FINISHED_MAPS, jobDetails.getFinishedMaps());
    addExpectedKey(JobHistoryKeys.FINISHED_REDUCES, jobDetails.getFinishedReduces());
    addExpectedKey(JobHistoryKeys.JOBID, jobDetails.getJobId());
    addExpectedKey(JobHistoryKeys.JOBNAME, jobDetails.getJobName());
    addExpectedKey(JobHistoryKeys.LAUNCH_TIME, jobDetails.getLaunchTime());
    addExpectedKey(JobHistoryKeys.JOB_PRIORITY, jobDetails.getPriority());
    addExpectedKey(JobHistoryKeys.SUBMIT_TIME, jobDetails.getSubmitTime());
    addExpectedKey(JobHistoryKeys.USER, jobDetails.getUser());
  }

  public Set<JobHistoryKeys> getUnassertedKeys() {
    Set<JobHistoryKeys> keys = new HashSet<JobHistoryKeys>();
    for (Pair<JobHistoryKeys, String> keyPair : keysToAssert) {
      keys.add(keyPair.getFirst());
    }
    return keys;
  }

  @Override
  public void handle(JobHistoryCopy.RecordTypes recType,
                     Map<JobHistoryKeys, String> values) throws IOException {
    if (JobHistoryCopy.RecordTypes.Job != recType) {
      log.warn(String.format("Not asserting record of type %s", recType));
      return;
    }
    String jobId = values.get(JobHistoryKeys.JOBID);
    log.info("Asserting record values for job "+jobId);

    if (values.get(JobHistoryKeys.JOB_STATUS) != null) {
      this.status = values.get(JobHistoryKeys.JOB_STATUS);
    }

    // assert first-class stats
    Iterator<Pair<JobHistoryKeys, String>> iterator = keysToAssert.iterator();
    while(iterator.hasNext()) {
      if(tryAssertKey(iterator.next(), values)) { iterator.remove(); }
    }

    // assert counters
    if (values.get(JobHistoryKeys.COUNTERS) != null) {
      assertCounters(jobId, values.get(JobHistoryKeys.COUNTERS), jobDetails.getCounters());
      assertedCounters = true;
    }
    if (values.get(JobHistoryKeys.MAP_COUNTERS) != null) {
      assertCounters(jobId, values.get(JobHistoryKeys.MAP_COUNTERS), jobDetails.getMapCounters());
      assertedMapCounters = true;
    }
    if (values.get(JobHistoryKeys.REDUCE_COUNTERS) != null) {
      assertCounters(jobId, values.get(JobHistoryKeys.REDUCE_COUNTERS), jobDetails.getReduceCounters());
      assertedReduceCounters = true;
    }
  }

  public boolean assertedAllCounters() {
    return assertedCounters && assertedMapCounters && assertedReduceCounters;
  }

  public String getFinalStatus() {
    return this.status;
  }

  private void addExpectedKey(JobHistoryKeys key, Object foundFalue) {
    String foundFalueString = foundFalue != null ? foundFalue.toString() : null;
    this.keysToAssert.add(new Pair<JobHistoryKeys, String>(key, foundFalueString));
  }

  private boolean tryAssertKey(Pair<JobHistoryKeys, String> keyValuePair,
                            Map<JobHistoryKeys, String> values) {
    JobHistoryKeys key = keyValuePair.getFirst();
    String foundValue = keyValuePair.getSecond();

    if (values.get(key) != null) {
      assertEquals(String.format("Unexpected value found for key %s", key.name()),
        values.get(key), foundValue);
      return true;
    }
    return false;
  }

  private void assertCounters(String jobId, String expectedEncodedCounters, CounterMap foundCounterMap) {
    assertNotNull(foundCounterMap);

    Counters expCounters = null;
    try {
      expCounters = Counters.fromEscapedCompactString(expectedEncodedCounters);
    } catch (ParseException e) {
      fail("Excetion trying to parse counters: " + e.getMessage());
    }

    for (Counters.Group group : expCounters) {
      String expGroupName = group.getName();
      for (Counters.Counter counter : group) {
        String expName = counter.getName();
        long expValue = counter.getValue();

        Counter foundCounter = foundCounterMap.getCounter(expGroupName, expName);
        assertNotNull(String.format(
          "Counter not found for job=%s, group=%s, name=%s", jobId, expGroupName, expName), foundCounter);
        assertEquals(String.format("Unexpected counter group"),
          expGroupName, foundCounter.getGroup());
        assertEquals(String.format("Unexpected counter name for job=%s, group=%s", jobId, expGroupName),
          expName, foundCounter.getKey());
        assertEquals(String.format("Unexpected counter value for job=%s, group=%s, name=%s", jobId, expGroupName, expName),
          expValue, foundCounter.getValue());
      }
    }
  }
}
