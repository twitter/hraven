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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.hraven.Flow;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.datasource.HRavenTestUtil;
import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.rest.ObjectMapperProvider;
import com.twitter.hraven.rest.RestJSONResource;
import com.twitter.hraven.rest.SerializationContext;
import com.twitter.hraven.util.JSONUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that we can deserialize json and serialize it again and get the same results. Written so
 * we can swap in new JSON content from the API and verify that serde still works.
 *
 */
@SuppressWarnings("deprecation")
public class TestJsonSerde {
  @SuppressWarnings("unused")
private static final Log LOG = LogFactory.getLog(TestJsonSerde.class);
  private static HBaseTestingUtility UTIL;
  private static HTable historyTable;
  private static JobHistoryByIdService idService;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);
    historyTable = new HTable(UTIL.getConfiguration(), Constants.HISTORY_TABLE_BYTES);
    idService = new JobHistoryByIdService(UTIL.getConfiguration());
  }

  @Test
  public void testJsonSerializationFlowStatsJobDetails() throws Exception {

    // load a sample flow
    final short numJobsAppOne = 3 ;
    final short numJobsAppTwo = 4 ;
    final long baseStats = 10L ;

    GenerateFlowTestData flowDataGen = new GenerateFlowTestData();
    flowDataGen.loadFlow("c1@local", "buser", "AppOne", 1234, "a", numJobsAppOne, baseStats,idService, historyTable);
    flowDataGen.loadFlow("c2@local", "Muser", "AppTwo", 2345, "b", numJobsAppTwo, baseStats,idService, historyTable);
    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    List<Flow> actualFlows = service.getFlowTimeSeriesStats("c1@local", "buser", "AppOne", "", 0L, 0L, 1000, null);

    // serialize flows into json
    ObjectMapper om = ObjectMapperProvider.createCustomMapper();
    om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    ByteArrayOutputStream f = new ByteArrayOutputStream();
    om.writeValue(f, actualFlows);
     ByteArrayInputStream is = new ByteArrayInputStream(f.toByteArray());
    @SuppressWarnings("unchecked")
    List<Flow> deserFlows = (List<Flow>) JSONUtil.readJson(is, new TypeReference<List<Flow>>() {});
    assertFlowDetails(actualFlows, deserFlows);

  }

  @Test
  public void testSerializationContext() throws Exception {

    // load a sample flow
    final short numJobs = 3 ;

    GenerateFlowTestData flowDataGen = new GenerateFlowTestData();
    // custom config to test out filtering of specific properties
    Map<String,String> fullConfig = Maps.newHashMap();
    fullConfig.put("name", "first");
    fullConfig.put("shortprop", "brief");
    fullConfig.put("longprop",
        "an extended bit of text that we will want to filter out from results");
    List<String> serializedKeys = Lists.newArrayList("name", "shortprop");

    flowDataGen.loadFlow("c1@local", "buser", "testSerializationContext", 1234, "a", numJobs, 10,
        idService, historyTable, fullConfig);

    JobHistoryService service = new JobHistoryService(UTIL.getConfiguration());
    Flow actualFlow = service.getFlow("c1@local", "buser", "testSerializationContext",
        1234, false);
    assertNotNull(actualFlow);
    Configuration actualConfig = actualFlow.getJobs().get(0).getConfiguration();
    assertEquals(fullConfig.get("name"), actualConfig.get("name"));
    assertEquals(fullConfig.get("shortprop"), actualConfig.get("shortprop"));
    assertEquals(fullConfig.get("longprop"), actualConfig.get("longprop"));

    // test serialization matching specific property keys
    // serialize flow into json
    RestJSONResource.serializationContext.set(
        new SerializationContext(SerializationContext.DetailLevel.EVERYTHING,
            new SerializationContext.ConfigurationFilter(serializedKeys)));
    ObjectMapper om = ObjectMapperProvider.createCustomMapper();
    om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    ByteArrayOutputStream f = new ByteArrayOutputStream();
    om.writeValue(f, actualFlow);
    ByteArrayInputStream is = new ByteArrayInputStream(f.toByteArray());
    Flow deserFlow = (Flow) JSONUtil.readJson(is, new TypeReference<Flow>() {});
    assertFlowEquals(actualFlow, deserFlow);
    // only config properties in serializedKeys should be present in the deserialized flow
    Configuration deserConfig = deserFlow.getJobs().get(0).getConfiguration();
    assertEquals(fullConfig.get("name"), deserConfig.get("name"));
    assertEquals(fullConfig.get("shortprop"), deserConfig.get("shortprop"));
    // longprop should not have been serialized
    assertNull(deserConfig.get("longprop"));

    // test serialization matching property regexes
    List<String> patterns = Lists.newArrayList("^.*prop$");
    RestJSONResource.serializationContext.set(
        new SerializationContext(SerializationContext.DetailLevel.EVERYTHING,
            new SerializationContext.RegexConfigurationFilter(patterns)));
    om = ObjectMapperProvider.createCustomMapper();
    om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    f = new ByteArrayOutputStream();
    om.writeValue(f, actualFlow);
    is = new ByteArrayInputStream(f.toByteArray());
    deserFlow = (Flow) JSONUtil.readJson(is, new TypeReference<Flow>() {});
    assertFlowEquals(actualFlow, deserFlow);
    // only config properties in serializedKeys should be present in the deserialized flow
    deserConfig = deserFlow.getJobs().get(0).getConfiguration();
    // only 2 *prop keys should be present
    assertNull(deserConfig.get("name"));
    assertEquals(fullConfig.get("shortprop"), deserConfig.get("shortprop"));
    assertEquals(fullConfig.get("longprop"), deserConfig.get("longprop"));
  }

  private void assertFlowDetails( List<Flow> flow1, List<Flow> flow2) {
    assertNotNull(flow1);
    assertNotNull(flow2);
    assertEquals(flow1.size(), flow2.size());
    assertTrue(flow1.equals(flow2));
    for(int i=0; i< flow1.size();i ++) {
      assertFlowEquals(flow1.get(i), flow2.get(i));
    }
  }

  private void assertFlowEquals(Flow flow1, Flow flow2) {
    assertEquals(flow1.getJobCount(), flow2.getJobCount());
    assertEquals(flow1.getJobs(), flow2.getJobs());
    assertEquals(flow1.getAppId(), flow2.getAppId());
    assertEquals(flow1.getCluster(), flow2.getCluster());
    assertEquals(flow1.getSubmitTime(), flow2.getSubmitTime());
    assertEquals(flow1.getDuration(), flow2.getDuration());
    assertEquals(flow1.getWallClockTime(), flow2.getWallClockTime());
    assertEquals(flow1.getRunId(), flow2.getRunId());
    assertEquals(flow1.getMapSlotMillis(), flow2.getMapSlotMillis());
    assertEquals(flow1.getReduceSlotMillis(), flow2.getReduceSlotMillis());
    assertEquals(flow1.getMegabyteMillis(), flow2.getMegabyteMillis());
    assertEquals(flow1.getHdfsBytesRead(), flow2.getHdfsBytesRead());
    assertEquals(flow1.getHdfsBytesWritten(), flow2.getHdfsBytesWritten());
    assertEquals(flow1.getJobGraphJSON(), flow2.getJobGraphJSON());
    assertEquals(flow1.getMapFileBytesRead(), flow2.getMapFileBytesRead());
    assertEquals(flow1.getMapFileBytesWritten(), flow2.getMapFileBytesWritten());
    assertEquals(flow1.getReduceFileBytesRead(), flow2.getReduceFileBytesRead());
    assertEquals(flow1.getTotalMaps(), flow2.getTotalMaps());
    assertEquals(flow1.getTotalReduces(), flow2.getTotalReduces());
    assertEquals(flow1.getVersion(), flow2.getVersion());
    assertEquals(flow1.getHistoryFileType(), flow2.getHistoryFileType());
    assertEquals(flow1.getUserName(), flow2.getUserName());
    assertJobListEquals(flow1.getJobs(), flow2.getJobs());
  }

  private void assertJobListEquals( List<JobDetails> job1, List<JobDetails> job2) {
    assertNotNull(job1);
    assertNotNull(job2);
    assertEquals(job1.size(), job2.size());

    for(int j=0; j<job1.size();j++) {
      assertEquals(job1.get(j).getJobId(), job2.get(j).getJobId());
      assertEquals(job1.get(j).getJobKey(), job2.get(j).getJobKey());
      assertEquals(job1.get(j).getMapFileBytesRead(), job2.get(j).getMapFileBytesRead());
      assertEquals(job1.get(j).getMapFileBytesWritten(), job2.get(j).getMapFileBytesWritten());
      assertEquals(job1.get(j).getReduceFileBytesRead(), job2.get(j).getReduceFileBytesRead());
      assertEquals(job1.get(j).getHdfsBytesRead(), job2.get(j).getHdfsBytesRead());
      assertEquals(job1.get(j).getHdfsBytesWritten(), job2.get(j).getHdfsBytesWritten());
      assertEquals(job1.get(j).getRunTime(), job2.get(j).getRunTime());
      assertEquals(job1.get(j).getMapSlotMillis(), job2.get(j).getMapSlotMillis());
      assertEquals(job1.get(j).getReduceSlotMillis(), job2.get(j).getReduceSlotMillis());
      assertEquals(job1.get(j).getMegabyteMillis(), job2.get(j).getMegabyteMillis());
      assertEquals(job1.get(j).getHistoryFileType(), job2.get(j).getHistoryFileType());
      assertEquals(job1.get(j).getUser(), job2.get(j).getUser());
    }
  }

  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

}
