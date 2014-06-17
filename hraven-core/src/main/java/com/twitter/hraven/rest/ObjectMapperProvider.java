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
package com.twitter.hraven.rest;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Predicate;
import com.twitter.hraven.AppSummary;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Counter;
import com.twitter.hraven.CounterMap;
import com.twitter.hraven.Flow;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.QualifiedPathKey;

/**
 * Class that provides custom JSON bindings (where needed) for out object model.
 */
@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {
  private final ObjectMapper customMapper;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(ObjectMapperProvider.class);

  public ObjectMapperProvider() {
    customMapper = createCustomMapper();
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return customMapper;
  }

  public static ObjectMapper createCustomMapper() {
    ObjectMapper result = new ObjectMapper();
    result.configure(Feature.INDENT_OUTPUT, true);
    SimpleModule module = new SimpleModule("hRavenModule", new Version(0, 4, 0, null));
    addJobMappings(module);
    module.addSerializer(Flow.class, new FlowSerializer());
    module.addSerializer(AppSummary.class, new AppSummarySerializer());
    result.registerModule(module);
    return result;
  }

  private static SimpleModule addJobMappings(SimpleModule module) {
    module.addSerializer(Configuration.class, new ConfigurationSerializer());
    module.addSerializer(CounterMap.class, new CounterSerializer());
    module.addSerializer(HdfsStats.class, new HdfsStatsSerializer());
    return module;
  }

  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  public static class ConfigurationSerializer extends JsonSerializer<Configuration> {

    @Override
    public void serialize(Configuration conf, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
      SerializationContext context = RestJSONResource.serializationContext.get();
      Predicate<String> configFilter = context.getConfigurationFilter();
      Iterator<Map.Entry<String, String>> keyValueIterator = conf.iterator();

      jsonGenerator.writeStartObject();

      // here's where we can filter out keys if we want
      while (keyValueIterator.hasNext()) {
        Map.Entry<String, String> kvp = keyValueIterator.next();
        if (configFilter == null || configFilter.apply(kvp.getKey())) {
          jsonGenerator.writeFieldName(kvp.getKey());
          jsonGenerator.writeString(kvp.getValue());
        }
      }
      jsonGenerator.writeEndObject();
    }
  }

  /**
   * Custom serializer for HdfsStats object
   */
  public static class HdfsStatsSerializer extends JsonSerializer<HdfsStats> {

    @Override
    public void serialize(HdfsStats hdfsStats, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {

      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("hdfsStatsKey");
      HdfsStatsKey hk = hdfsStats.getHdfsStatsKey();
      QualifiedPathKey qpk = hk.getQualifiedPathKey();
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("cluster");
      jsonGenerator.writeNumber(qpk.getCluster());
      if (StringUtils.isNotBlank(qpk.getNamespace())) {
        jsonGenerator.writeFieldName("namespace");
        jsonGenerator.writeNumber(qpk.getNamespace());
      }
      jsonGenerator.writeFieldName("path");
      jsonGenerator.writeNumber(qpk.getPath());
      jsonGenerator.writeFieldName("runId");
      jsonGenerator.writeNumber(hk.getRunId());
      jsonGenerator.writeEndObject();
      jsonGenerator.writeFieldName("fileCount");
      jsonGenerator.writeNumber(hdfsStats.getFileCount());
      jsonGenerator.writeFieldName("dirCount");
      jsonGenerator.writeNumber(hdfsStats.getDirCount());
      jsonGenerator.writeFieldName("spaceConsumed");
      jsonGenerator.writeNumber(hdfsStats.getSpaceConsumed());
      jsonGenerator.writeFieldName("owner");
      jsonGenerator.writeString(hdfsStats.getOwner());
      jsonGenerator.writeFieldName("quota");
      jsonGenerator.writeNumber(hdfsStats.getQuota());
      jsonGenerator.writeFieldName("spaceQuota");
      jsonGenerator.writeNumber(hdfsStats.getSpaceQuota());
      jsonGenerator.writeFieldName("trashFileCount");
      jsonGenerator.writeNumber(hdfsStats.getTrashFileCount());
      jsonGenerator.writeFieldName("trashSpaceConsumed");
      jsonGenerator.writeNumber(hdfsStats.getTrashSpaceConsumed());
      jsonGenerator.writeFieldName("tmpFileCount");
      jsonGenerator.writeNumber(hdfsStats.getTmpFileCount());
      jsonGenerator.writeFieldName("tmpSpaceConsumed");
      jsonGenerator.writeNumber(hdfsStats.getTmpSpaceConsumed());
      jsonGenerator.writeFieldName("accessCountTotal");
      jsonGenerator.writeNumber(hdfsStats.getAccessCountTotal());
      jsonGenerator.writeFieldName("accessCost");
      jsonGenerator.writeNumber(hdfsStats.getAccessCost());
      jsonGenerator.writeFieldName("storageCost");
      jsonGenerator.writeNumber(hdfsStats.getStorageCost());
      jsonGenerator.writeFieldName("hdfsCost");
      jsonGenerator.writeNumber(hdfsStats.getHdfsCost());

      jsonGenerator.writeEndObject();
    }
  }

  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  public static class CounterSerializer extends JsonSerializer<CounterMap> {

    @Override
    public void serialize(CounterMap counterMap, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {

      jsonGenerator.writeStartObject();
      for (String group : counterMap.getGroups()) {
        jsonGenerator.writeFieldName(group);

        jsonGenerator.writeStartObject();
        Map<String, Counter> groupMap = counterMap.getGroup(group);
        for (String counterName : groupMap.keySet()) {
          Counter counter = groupMap.get(counterName);
          jsonGenerator.writeFieldName(counter.getKey());
          jsonGenerator.writeNumber(counter.getValue());
        }
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndObject();
    }
  }

  /**
   * Custom serializer for Flow object. We don't want to serialize the
   * classLoader. based on the parameters passed by caller, we determine which
   * fields to include in serialized response
   */
  public static class FlowSerializer extends JsonSerializer<Flow> {
    @SuppressWarnings("deprecation")
    @Override
    public void serialize(Flow aFlow, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      SerializationContext.DetailLevel selectedSerialization =
          RestJSONResource.serializationContext.get().getLevel();
      if (selectedSerialization == SerializationContext.DetailLevel.EVERYTHING) {
        // should generate the json for everything in the flow object
        ObjectMapper om = new ObjectMapper();
        om.registerModule(
            addJobMappings(new SimpleModule("hRavenModule", new Version(0, 4, 0, null))));
        om.writeValue(jsonGenerator, aFlow);
      } else {
        if (selectedSerialization == SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_ONLY
            || selectedSerialization == SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_WITH_JOB_STATS) {
          jsonGenerator.writeStartObject();
          // serialize the FlowKey object
          jsonGenerator.writeFieldName("flowKey");
          jsonGenerator.writeObject(aFlow.getFlowKey());
          // serialize individual members of this class
          jsonGenerator.writeFieldName("flowName");
          jsonGenerator.writeString(aFlow.getFlowName());
          jsonGenerator.writeFieldName("userName");
          jsonGenerator.writeString(aFlow.getUserName());
          jsonGenerator.writeFieldName("jobCount");
          jsonGenerator.writeNumber(aFlow.getJobCount());
          jsonGenerator.writeFieldName("totalMaps");
          jsonGenerator.writeNumber(aFlow.getTotalMaps());
          jsonGenerator.writeFieldName("totalReduces");
          jsonGenerator.writeNumber(aFlow.getTotalReduces());
          jsonGenerator.writeFieldName("mapFilesBytesRead");
          jsonGenerator.writeNumber(aFlow.getMapFileBytesRead());
          jsonGenerator.writeFieldName("mapFilesBytesWritten");
          jsonGenerator.writeNumber(aFlow.getMapFileBytesWritten());
          jsonGenerator.writeFieldName("reduceFilesBytesRead");
          jsonGenerator.writeNumber(aFlow.getReduceFileBytesRead());
          jsonGenerator.writeFieldName("hdfsBytesRead");
          jsonGenerator.writeNumber(aFlow.getHdfsBytesRead());
          jsonGenerator.writeFieldName("hdfsBytesWritten");
          jsonGenerator.writeNumber(aFlow.getHdfsBytesWritten());
          jsonGenerator.writeFieldName("mapSlotMillis");
          jsonGenerator.writeNumber(aFlow.getMapSlotMillis());
          jsonGenerator.writeFieldName("reduceSlotMillis");
          jsonGenerator.writeNumber(aFlow.getReduceSlotMillis());
          jsonGenerator.writeFieldName("megabyteMillis");
          jsonGenerator.writeNumber(aFlow.getMegabyteMillis());
          jsonGenerator.writeFieldName("cost");
          jsonGenerator.writeNumber(aFlow.getCost());
          jsonGenerator.writeFieldName("reduceShuffleBytes");
          jsonGenerator.writeNumber(aFlow.getReduceShuffleBytes());
          jsonGenerator.writeFieldName("duration");
          jsonGenerator.writeNumber(aFlow.getDuration());
          jsonGenerator.writeFieldName("wallClockTime");
          jsonGenerator.writeNumber(aFlow.getWallClockTime());
          jsonGenerator.writeFieldName("cluster");
          jsonGenerator.writeString(aFlow.getCluster());
          jsonGenerator.writeFieldName("appId");
          jsonGenerator.writeString(aFlow.getAppId());
          jsonGenerator.writeFieldName("runId");
          jsonGenerator.writeNumber(aFlow.getRunId());
          jsonGenerator.writeFieldName("version");
          jsonGenerator.writeString(aFlow.getVersion());
          jsonGenerator.writeFieldName("hadoopVersion");
          /**
           *  unlikely that the next line with .toString 
           *  will throw NPE since Flow class always sets
           *  default hadoop version in Flow#addJob
           */
          jsonGenerator.writeString(aFlow.getHadoopVersion().toString());
          jsonGenerator.writeFieldName(Constants.HRAVEN_QUEUE);
          jsonGenerator.writeString(aFlow.getQueue());
          jsonGenerator.writeFieldName("counters");
          jsonGenerator.writeObject(aFlow.getCounters());
          jsonGenerator.writeFieldName("mapCounters");
          jsonGenerator.writeObject(aFlow.getMapCounters());
          jsonGenerator.writeFieldName("reduceCounters");
          jsonGenerator.writeObject(aFlow.getReduceCounters());
          // if flag, include job details
          if (selectedSerialization ==
              SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_WITH_JOB_STATS) {
            jsonGenerator.writeFieldName("jobs");
            jsonGenerator.writeObject(aFlow.getJobs());
          }
          jsonGenerator.writeEndObject();
        }
      }
    }
  }

  /**
   * Custom serializer for App object. We don't want to serialize the
   * classLoader. based on the parameters passed by caller, we determine which
   * fields to include in serialized response
   */
  public static class AppSummarySerializer extends JsonSerializer<AppSummary> {
    @Override
    public void serialize(AppSummary anApp, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      SerializationContext.DetailLevel selectedSerialization =
          RestJSONResource.serializationContext.get().getLevel();
      if (selectedSerialization == SerializationContext.DetailLevel.EVERYTHING) {
        // should generate the json for everything in the flow object
        ObjectMapper om = new ObjectMapper();
        om.registerModule(
            addJobMappings(new SimpleModule("hRavenModule", new Version(0, 4, 0, null))));
        om.writeValue(jsonGenerator, anApp);
      } else {
        if (selectedSerialization == SerializationContext.DetailLevel.APP_SUMMARY_STATS_NEW_JOBS_ONLY) {
          // should generate the json for everything in the flow object
          ObjectMapper om = new ObjectMapper();
          om.registerModule(
              addJobMappings(new SimpleModule("hRavenModule", new Version(0, 4, 0, null))));
          jsonGenerator.writeStartObject();
          jsonGenerator.writeFieldName("cluster");
          jsonGenerator.writeString(anApp.getKey().getCluster());
          jsonGenerator.writeFieldName("userName");
          jsonGenerator.writeString(anApp.getKey().getUserName());
          jsonGenerator.writeFieldName("appId");
          jsonGenerator.writeString(anApp.getKey().getAppId());
          jsonGenerator.writeFieldName("queue");
          jsonGenerator.writeObject(anApp.getQueue());
          jsonGenerator.writeFieldName("numberRuns");
          jsonGenerator.writeNumber(anApp.getNumberRuns());
          jsonGenerator.writeFieldName("firstRunId");
          jsonGenerator.writeNumber(anApp.getFirstRunId());
          jsonGenerator.writeFieldName("lastRunId");
          jsonGenerator.writeNumber(anApp.getLastRunId());
          jsonGenerator.writeEndObject();
        }
      }
    }
  }
}
