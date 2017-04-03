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
import java.util.ArrayList;


import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;

import com.google.common.base.Predicate;
import com.twitter.hraven.AppSummary;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Counter;
import com.twitter.hraven.CounterMap;
import com.twitter.hraven.Flow;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.QualifiedPathKey;
import com.twitter.hraven.TaskDetails;
import com.twitter.hraven.rest.SerializationContext.DetailLevel;

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

  /**
   * creates a new SimpleModule for holding the serializers
   * @return SimpleModule
   */
  private static SimpleModule createhRavenModule() {
    return new SimpleModule("hRavenModule", new Version(0, 4, 0, null));
  }

  public static ObjectMapper createCustomMapper() {
    ObjectMapper result = new ObjectMapper();
    result.configure(Feature.INDENT_OUTPUT, true);
    SimpleModule module = createhRavenModule();
    addJobMappings(module);
    module.addSerializer(Flow.class, new FlowSerializer());
    module.addSerializer(AppSummary.class, new AppSummarySerializer());
    module.addSerializer(TaskDetails.class, new TaskDetailsSerializer());
    module.addSerializer(JobDetails.class, new JobDetailsSerializer());
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
   * Custom serializer for Configuration object. We don't want to serialize the
   * classLoader.
   */
  public static class ConfigurationSerializer extends
      JsonSerializer<Configuration> {

    @Override
    public void serialize(Configuration conf, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      SerializationContext context = RestResource.serializationContext
          .get();
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
   * Custom serializer for TaskDetails object.
   */
  public static class TaskDetailsSerializer extends JsonSerializer<TaskDetails> {

    @Override
    public void serialize(TaskDetails td, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {

      SerializationContext context = RestResource.serializationContext
          .get();
      Predicate<String> includeFilter = context.getTaskFilter();
      Predicate<String> includeCounterFilter = context.getCounterFilter();

      if(includeCounterFilter != null && includeFilter == null) {
        includeFilter = new SerializationContext.FieldNameFilter(new ArrayList<String>());
      }

      if (includeFilter == null && includeCounterFilter == null) {
        // should generate the json for everything in the task details object
        ObjectMapper om = new ObjectMapper();
        om.registerModule(addJobMappings(createhRavenModule()));
        om.writeValue(jsonGenerator, td);
      } else {
        // should generate the json for everything in the task details object
        // as per the filtering criteria
        ObjectMapper om = new ObjectMapper();
        om.registerModule(addJobMappings(createhRavenModule()));
        jsonGenerator.writeStartObject();
        filteredWrite("taskKey", includeFilter, td.getTaskKey(), jsonGenerator);
        filteredWrite("taskId", includeFilter, td.getTaskId(), jsonGenerator);
        filteredWrite("startTime", includeFilter, td.getStartTime(),
            jsonGenerator);
        filteredWrite("finishTime", includeFilter, td.getFinishTime(),
            jsonGenerator);
        filteredWrite("taskType", includeFilter, td.getType(), jsonGenerator);
        filteredWrite("status", includeFilter, td.getStatus(), jsonGenerator);
        filteredWrite("splits", includeFilter, td.getSplits(), jsonGenerator);
        filteredCounterWrite("counters", includeFilter, includeCounterFilter,
            td.getCounters(), jsonGenerator);
        filteredWrite("taskAttemptId", includeFilter, td.getTaskAttemptId(),
            jsonGenerator);
        filteredWrite("trackerName", includeFilter, td.getTrackerName(),
            jsonGenerator);
        filteredWrite("hostname", includeFilter, td.getHostname(),
            jsonGenerator);
        filteredWrite("httpPort", includeFilter, td.getTrackerName(),
            jsonGenerator);
        filteredWrite("state", includeFilter, td.getState(), jsonGenerator);
        filteredWrite("error", includeFilter, td.getError(), jsonGenerator);
        filteredWrite("shuffleFinished", includeFilter,
            td.getShuffleFinished(), jsonGenerator);
        filteredWrite("sortFinished", includeFilter, td.getSortFinished(),
            jsonGenerator);
        jsonGenerator.writeEndObject();
      }
    }

  }

  /**
   * Custom serializer for JobDetails object.
   */
  public static class JobDetailsSerializer extends JsonSerializer<JobDetails> {

    @Override
    public void serialize(JobDetails jd, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      SerializationContext context = RestResource.serializationContext
          .get();
      Predicate<String> includeFilter = context.getJobFilter();
      Predicate<String> includeCounterFilter = context.getCounterFilter();

      if (includeCounterFilter != null && includeFilter == null){
        includeFilter = new SerializationContext.FieldNameFilter(new ArrayList<String>());
      }

      if (includeFilter == null && includeCounterFilter == null) {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(addJobMappings(createhRavenModule()));
        om.writeValue(jsonGenerator, jd);
      } else {
        // should generate the json for every field in the job details object
        // as per the filtering criteria
        ObjectMapper om = new ObjectMapper();
        om.registerModule(addJobMappings(createhRavenModule()));
        jsonGenerator.writeStartObject();
        filteredWrite("jobKey", includeFilter, jd.getJobKey(), jsonGenerator);
        filteredWrite("jobId", includeFilter, jd.getJobId(), jsonGenerator);
        filteredWrite("jobName", includeFilter, jd.getJobName(), jsonGenerator);
        filteredWrite("user", includeFilter, jd.getUser(), jsonGenerator);
        filteredWrite("priority", includeFilter, jd.getPriority(),
            jsonGenerator);
        filteredWrite("status", includeFilter, jd.getStatus(), jsonGenerator);
        filteredWrite("version", includeFilter, jd.getVersion(), jsonGenerator);
        filteredWrite("hadoopVersion", includeFilter, jd.getHadoopVersion(),
            jsonGenerator);
        filteredWrite("queue", includeFilter, jd.getQueue(), jsonGenerator);
        filteredWrite("submitTime", includeFilter, jd.getSubmitTime(),
            jsonGenerator);
        filteredWrite("launchTime", includeFilter, jd.getLaunchTime(),
            jsonGenerator);
        filteredWrite("finishTime", includeFilter, jd.getFinishTime(),
            jsonGenerator);
        filteredWrite("totalMaps", includeFilter, jd.getTotalMaps(),
            jsonGenerator);
        filteredWrite("totalReduces", includeFilter, jd.getTotalReduces(),
            jsonGenerator);
        filteredWrite("finishedMaps", includeFilter, jd.getFinishedMaps(),
            jsonGenerator);
        filteredWrite("finishedReduces", includeFilter,
            jd.getFinishedReduces(), jsonGenerator);
        filteredWrite("failedMaps", includeFilter, jd.getFailedMaps(),
            jsonGenerator);
        filteredWrite("failedReduces", includeFilter, jd.getFailedReduces(),
            jsonGenerator);
        filteredWrite("mapFileBytesRead", includeFilter,
            jd.getMapFileBytesRead(), jsonGenerator);
        filteredWrite("mapFileBytesWritten", includeFilter,
            jd.getMapFileBytesWritten(), jsonGenerator);
        filteredWrite("reduceFileBytesRead", includeFilter,
            jd.getReduceFileBytesRead(), jsonGenerator);
        filteredWrite("hdfsBytesRead", includeFilter, jd.getHdfsBytesRead(),
            jsonGenerator);
        filteredWrite("hdfsBytesWritten", includeFilter,
            jd.getHdfsBytesWritten(), jsonGenerator);
        filteredWrite("mapSlotMillis", includeFilter, jd.getMapSlotMillis(),
            jsonGenerator);
        filteredWrite("reduceSlotMillis", includeFilter,
            jd.getReduceSlotMillis(), jsonGenerator);
        filteredWrite("reduceShuffleBytes", includeFilter,
            jd.getReduceShuffleBytes(), jsonGenerator);
        filteredWrite("megabyteMillis", includeFilter, jd.getMegabyteMillis(),
            jsonGenerator);
        filteredWrite("cost", includeFilter, jd.getCost(), jsonGenerator);
        filteredWrite("configuration", includeFilter, jd.getConfiguration(), jsonGenerator);

        filteredCounterWrite("counters", includeFilter, includeCounterFilter,
            jd.getCounters(), jsonGenerator);
        filteredCounterWrite("mapCounters", includeFilter, includeCounterFilter,
            jd.getMapCounters(), jsonGenerator);
        filteredCounterWrite("reduceCounters", includeFilter, includeCounterFilter,
            jd.getReduceCounters(), jsonGenerator);
        jsonGenerator.writeEndObject();
      }
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
      jsonGenerator.writeString(qpk.getCluster());
      if (StringUtils.isNotBlank(qpk.getNamespace())) {
        jsonGenerator.writeFieldName("namespace");
        jsonGenerator.writeString(qpk.getNamespace());
      }
      jsonGenerator.writeFieldName("path");
      jsonGenerator.writeString(qpk.getPath());
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
   * Custom serializer for Configuration object. We don't want to serialize the
   * classLoader.
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
    @Override
    public void serialize(Flow aFlow, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {

      SerializationContext context = RestResource.serializationContext.get();
      SerializationContext.DetailLevel selectedSerialization = context.getLevel();
      Predicate<String> includeFilter = context.getFlowFilter();
      writeFlowDetails(jsonGenerator, aFlow, selectedSerialization, includeFilter);
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
      SerializationContext.DetailLevel selectedSerialization = RestResource.serializationContext
          .get().getLevel();
      if (selectedSerialization == SerializationContext.DetailLevel.EVERYTHING) {
        // should generate the json for everything in the app summary object
        ObjectMapper om = new ObjectMapper();
        om.registerModule(addJobMappings(createhRavenModule()));
        om.writeValue(jsonGenerator, anApp);
      } else {
        if (selectedSerialization == SerializationContext.DetailLevel.APP_SUMMARY_STATS_NEW_JOBS_ONLY) {
          // should generate the json for stats relevant for new jobs
          ObjectMapper om = new ObjectMapper();
          om.registerModule(addJobMappings(createhRavenModule()));
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
        } else if (selectedSerialization == SerializationContext.DetailLevel.APP_SUMMARY_STATS_ALL_APPS) {
          // should generate the json for everything in the app summary object
          ObjectMapper om = new ObjectMapper();
          om.registerModule(addJobMappings(createhRavenModule()));
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
          jsonGenerator.writeFieldName("totalMaps");
          jsonGenerator.writeNumber(anApp.getTotalMaps());
          jsonGenerator.writeFieldName("totalReduces");
          jsonGenerator.writeNumber(anApp.getTotalReduces());
          jsonGenerator.writeFieldName("mapSlotMillis");
          jsonGenerator.writeNumber(anApp.getMapSlotMillis());
          jsonGenerator.writeFieldName("reduceSlotMillis");
          jsonGenerator.writeNumber(anApp.getReduceSlotMillis());
          jsonGenerator.writeFieldName("megaByteMillis");
          jsonGenerator.writeNumber(anApp.getMbMillis());
          jsonGenerator.writeFieldName("jobCount");
          jsonGenerator.writeNumber(anApp.getJobCount());
          jsonGenerator.writeFieldName("dailyCost");
          jsonGenerator.writeNumber(anApp.getCost());
          jsonGenerator.writeEndObject();
        }

      }
    }
  }

  /**
   * checks if the member is to be filtered out or no if filter itself is
   * null, writes out that member
   *
   * @param member
   * @param includeFilter
   * @param taskObject
   * @param jsonGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  public static void filteredWrite(String member, Predicate<String> includeFilter,
      Object taskObject, JsonGenerator jsonGenerator)
      throws JsonGenerationException, IOException {
    if (includeFilter != null) {
      if (includeFilter.apply(member)) {
        jsonGenerator.writeFieldName(member);
        jsonGenerator.writeObject(taskObject);
      }
    } else {
      jsonGenerator.writeFieldName(member);
      jsonGenerator.writeObject(taskObject);
    }
  }

  /**
   * checks if the member is to be filtered out or no if filter itself is
   * null, writes out that member as a String
   *
   * @param member
   * @param includeFilter
   * @param taskObject
   * @param jsonGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  public static void filteredWrite(String member, Predicate<String> includeFilter,
      String taskObject, JsonGenerator jsonGenerator)
      throws JsonGenerationException, IOException {
    if (includeFilter != null) {
      if (includeFilter.apply(member)) {
        jsonGenerator.writeFieldName(member);
        jsonGenerator.writeString(taskObject);
      }
    } else {
      jsonGenerator.writeFieldName(member);
      jsonGenerator.writeString(taskObject);
    }
  }


  /**
   * checks if the member is to be filtered out or no if filter itself is
   * null, writes out that member
   *
   * @param member
   * @param includeFilter
   * @param taskObject
   * @param jsonGenerator
   * @throws JsonGenerationException
   * @throws IOException
   */
  public static void filteredCounterWrite(String member, Predicate<String> includeFilter,
                                          Predicate<String> includeCounterFilter,
                                   CounterMap counterMap, JsonGenerator jsonGenerator)
      throws JsonGenerationException, IOException {
    if (includeFilter != null && includeCounterFilter == null) {
      if (includeFilter.apply(member)) {
        jsonGenerator.writeFieldName(member);
        jsonGenerator.writeObject(counterMap);
      }
    } else {
      if(includeCounterFilter != null) {
        // get group name, counter name,
        // check if it is wanted
        // if yes print it.
        boolean startObjectGroupMap = false;
        jsonGenerator.writeFieldName(member);

        String fullCounterName;
        jsonGenerator.writeStartObject();

        for (String group : counterMap.getGroups()) {
          Map<String, Counter> groupMap = counterMap.getGroup(group);
          for (String counterName : groupMap.keySet()) {
            Counter counter = groupMap.get(counterName);
            fullCounterName = group + "." + counter.getKey();
            if(includeCounterFilter.apply(fullCounterName)) {
              if(startObjectGroupMap == false) {
                jsonGenerator.writeFieldName(group);
                jsonGenerator.writeStartObject();
                startObjectGroupMap = true;
              }
              jsonGenerator.writeFieldName(counter.getKey());
              jsonGenerator.writeNumber(counter.getValue());
            }
          }
          if(startObjectGroupMap) {
            jsonGenerator.writeEndObject();
            startObjectGroupMap = false;
          }
        }
          jsonGenerator.writeEndObject();
      }
    }
  }

  /**
   * Writes out the flow object
   *
   * @param jsonGenerator
   * @param aFlow
   * @param selectedSerialization
   * @param includeFilter
   * @param includeJobFieldFilter
   * @throws JsonGenerationException
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static void writeFlowDetails(JsonGenerator jsonGenerator, Flow aFlow,
      DetailLevel selectedSerialization, Predicate<String> includeFilter)
      throws JsonGenerationException, IOException {
    jsonGenerator.writeStartObject();
    // serialize the FlowKey object
    filteredWrite("flowKey", includeFilter, aFlow.getFlowKey(), jsonGenerator);

    // serialize individual members of this class
    filteredWrite("flowName", includeFilter, aFlow.getFlowName(), jsonGenerator);
    filteredWrite("userName", includeFilter, aFlow.getUserName(), jsonGenerator);
    filteredWrite("jobCount", includeFilter, aFlow.getJobCount(), jsonGenerator);
    filteredWrite("totalMaps", includeFilter, aFlow.getTotalMaps(), jsonGenerator);
    filteredWrite("totalReduces", includeFilter, aFlow.getTotalReduces(), jsonGenerator);
    filteredWrite("mapFileBytesRead", includeFilter, aFlow.getMapFileBytesRead(), jsonGenerator);
    filteredWrite("mapFileBytesWritten", includeFilter, aFlow.getMapFileBytesWritten(), jsonGenerator);
    filteredWrite("reduceFileBytesRead", includeFilter, aFlow.getReduceFileBytesRead(), jsonGenerator);
    filteredWrite("hdfsBytesRead", includeFilter, aFlow.getHdfsBytesRead(), jsonGenerator);
    filteredWrite("hdfsBytesWritten", includeFilter, aFlow.getHdfsBytesWritten(), jsonGenerator);
    filteredWrite("mapSlotMillis", includeFilter, aFlow.getMapSlotMillis(), jsonGenerator);
    filteredWrite("reduceSlotMillis", includeFilter, aFlow.getReduceSlotMillis(), jsonGenerator);
    filteredWrite("megabyteMillis", includeFilter, aFlow.getMegabyteMillis(), jsonGenerator);
    filteredWrite("cost", includeFilter, aFlow.getCost(), jsonGenerator);
    filteredWrite("reduceShuffleBytes", includeFilter, aFlow.getReduceShuffleBytes(), jsonGenerator);
    filteredWrite("duration", includeFilter, aFlow.getDuration(), jsonGenerator);
    filteredWrite("wallClockTime", includeFilter, aFlow.getWallClockTime(), jsonGenerator);
    filteredWrite("cluster", includeFilter, aFlow.getCluster(), jsonGenerator);
    filteredWrite("appId", includeFilter, aFlow.getAppId(), jsonGenerator);
    filteredWrite("runId", includeFilter, aFlow.getRunId(), jsonGenerator);
    filteredWrite("version", includeFilter, aFlow.getVersion(), jsonGenerator);
    filteredWrite("hadoopVersion", includeFilter, aFlow.getHadoopVersion(), jsonGenerator);
    if (selectedSerialization == SerializationContext.DetailLevel.EVERYTHING) {
      filteredWrite("submitTime", includeFilter, aFlow.getSubmitTime(), jsonGenerator);
      filteredWrite("launchTime", includeFilter, aFlow.getLaunchTime(), jsonGenerator);
      filteredWrite("finishTime", includeFilter, aFlow.getFinishTime(), jsonGenerator);
    }
    filteredWrite(Constants.HRAVEN_QUEUE, includeFilter, aFlow.getQueue(), jsonGenerator);
    filteredWrite("counters", includeFilter, aFlow.getCounters(), jsonGenerator);
    filteredWrite("mapCounters", includeFilter, aFlow.getMapCounters(), jsonGenerator);
    filteredWrite("reduceCounters", includeFilter, aFlow.getReduceCounters(), jsonGenerator);

    // if flag, include job details
    if ((selectedSerialization == SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_WITH_JOB_STATS)
        || (selectedSerialization == SerializationContext.DetailLevel.EVERYTHING)) {
      jsonGenerator.writeFieldName("jobs");
      jsonGenerator.writeObject(aFlow.getJobs());
    }
    jsonGenerator.writeEndObject();

  }
}
