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
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.twitter.hraven.etl.ProcessRecord;
import com.twitter.hraven.etl.ProcessState;

/**
 * Test {@link ProcessRecord}, and specifically the key construction and
 * deconstruction.s
 * 
 */
public class TestProcessRecord {

  private static final String CLUSTER = "cluster@identifier";
  private static final ProcessState PROCESS_STATE = ProcessState.CREATED;
  private static final long MIN_MODIFICATION_TIME_MILLIS = 1336115621494L;
  private static final long MAX_MODIFICATION_TIME_MILLIS = 1336115732505L;
  private static final int PROCESSED_JOB_FILES = 7;
  private static final String PROCESSING_DIRECTORY = "/hadoop/mapred/history/processing/20120503061229";

  @Test
  public void testConstructors() {
    ProcessRecord processRecord = new ProcessRecord(CLUSTER,
        MIN_MODIFICATION_TIME_MILLIS, MAX_MODIFICATION_TIME_MILLIS,
        PROCESSED_JOB_FILES, PROCESSING_DIRECTORY);
    ProcessRecord processRecord2 = new ProcessRecord(
        processRecord.getCluster(), PROCESS_STATE,
        processRecord.getMinModificationTimeMillis(),
        processRecord.getMaxModificationTimeMillis(),
        processRecord.getProcessedJobFiles(),
        processRecord.getProcessFile(), null, null);

    assertEquals(processRecord.getKey(), processRecord2.getKey());
    assertEquals(processRecord.getCluster(), processRecord2.getCluster());
    assertEquals(processRecord.getMaxModificationTimeMillis(),
        processRecord2.getMaxModificationTimeMillis());
    assertEquals(processRecord.getMinModificationTimeMillis(),
        processRecord2.getMinModificationTimeMillis());
    assertEquals(processRecord.getProcessedJobFiles(),
        processRecord2.getProcessedJobFiles());
    assertEquals(processRecord.getProcessFile(),
        processRecord2.getProcessFile());
    assertEquals(processRecord.getMinJobId(),
        processRecord2.getMinJobId());
    assertEquals(processRecord.getMaxJobId(),
        processRecord2.getMaxJobId());
    

    assertEquals(CLUSTER, processRecord2.getCluster());
    assertEquals(MAX_MODIFICATION_TIME_MILLIS,
        processRecord2.getMaxModificationTimeMillis());
    assertEquals(MIN_MODIFICATION_TIME_MILLIS,
        processRecord2.getMinModificationTimeMillis());
    assertEquals(PROCESSED_JOB_FILES, processRecord2.getProcessedJobFiles());
    assertEquals(PROCESSING_DIRECTORY, processRecord2.getProcessFile());
    assertNull(processRecord2.getMinJobId());
    
    // TODO: Add a minJobId and maxJobId value test

  }

}
