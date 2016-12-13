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
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.sun.jersey.core.util.Base64;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.twitter.hraven.AppSummary;
import com.twitter.hraven.Cluster;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStats;
import com.twitter.hraven.HravenResponseMetrics;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.TaskDetails;
import com.twitter.hraven.datasource.AppSummaryService;
import com.twitter.hraven.datasource.AppVersionService;
import com.twitter.hraven.datasource.FlowKeyConverter;
import com.twitter.hraven.datasource.HdfsStatsService;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.VersionInfo;
import com.twitter.hraven.util.StringUtil;

/**
 * Main REST resource that handles binding the REST API to the JobHistoryService.
 *
 * TODO: better prevalidation
 * TODO: handle null results with empty json object or response code
 */
@Path("/api/v1/")
public class RestJSONResource {
  private static final Log LOG = LogFactory.getLog(RestJSONResource.class);
  public static final String SLASH = "/" ;

  private static final Configuration HBASE_CONF = HBaseConfiguration.create();
  private static final ThreadLocal<JobHistoryService> serviceThreadLocal =
    new ThreadLocal<JobHistoryService>() {

    @Override
    protected JobHistoryService initialValue() {
      try {
        LOG.info("Initializing JobHistoryService");
        return new JobHistoryService(HBASE_CONF);
      } catch (IOException e) {
        throw new RuntimeException("Could not initialize JobHistoryService", e);
      }
    }
  };

  private static final ThreadLocal<AppVersionService> serviceThreadLocalAppVersion =
      new ThreadLocal<AppVersionService>() {

      @Override
      protected AppVersionService initialValue() {
        try {
          LOG.info("Initializing AppVersionService");
          return new AppVersionService(HBASE_CONF);
        } catch (IOException e) {
          throw new RuntimeException("Could not initialize AppVersionService", e);
        }
      }
    };

  private static final ThreadLocal<AppSummaryService> serviceThreadLocalAppService =
        new ThreadLocal<AppSummaryService>() {

        @Override
        protected AppSummaryService initialValue() {
          try {
            LOG.info("Initializing AppService");
            return new AppSummaryService(HBASE_CONF);
          } catch (IOException e) {
            throw new RuntimeException("Could not initialize AppService", e);
          }
        }
      };

    private static final ThreadLocal<HdfsStatsService> serviceThreadLocalHdfsStats =
        new ThreadLocal<HdfsStatsService>() {

        @Override
        protected HdfsStatsService initialValue() {
          try {
            LOG.info("Initializing HdfsStatsService");
            return new HdfsStatsService(HBASE_CONF);
          } catch (IOException e) {
            throw new RuntimeException("Could not initialize HdfsStatsService", e);
          }
        }
      };

  public static final ThreadLocal<SerializationContext> serializationContext =
                                new ThreadLocal<SerializationContext>() {
    @Override
    protected SerializationContext initialValue() {
      // by default all retrieved data is serialized, overrideable per endpoint
      return new SerializationContext(SerializationContext.DetailLevel.EVERYTHING);
    }
  };

  @GET
  @Path("job/{cluster}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public JobDetails getJobById(@PathParam("cluster") String cluster,
                               @PathParam("jobId") String jobId,
                               @QueryParam("include") List<String> includeFields,
                               @QueryParam("includeCounter") List<String>
                                     includeCounter)
                                   throws IOException {
    LOG.info("Fetching JobDetails for jobId=" + jobId);
    Stopwatch timer = new Stopwatch().start();
    Predicate<String> includeFilter = null;
    if (includeFields != null && !includeFields.isEmpty()) {
      includeFilter = new SerializationContext.FieldNameFilter(includeFields);
    }

    Predicate<String> includeCounterFilter = null;
    if (includeCounter != null && !includeCounter.isEmpty()) {
      includeCounterFilter = new SerializationContext.FieldNameFilter(includeCounter);
    }

    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.EVERYTHING, null, null, includeFilter, null));
    JobDetails jobDetails = getJobHistoryService().getJobByJobID(cluster, jobId);
    timer.stop();
    if (jobDetails != null) {
      LOG.info("For job/{cluster}/{jobId} with input query:"
          + " job/" + cluster + SLASH + jobId + "&"
          + StringUtil.buildParam("include", includeFields)
          + " fetched jobDetails for "
          + jobDetails.getJobName() + " in " + timer);
    } else {
      LOG.info("For job/{cluster}/{jobId} with input query:" + " job/"
          + cluster + SLASH + jobId + "&"
          + StringUtil.buildParam("include",includeFields)
          + " No jobDetails found, but spent " + timer);
    }
    // export latency metrics
    HravenResponseMetrics.JOB_API_LATENCY_VALUE
      .set(timer.elapsed(TimeUnit.MILLISECONDS));

    return jobDetails;

  }

  @GET
  @Path("tasks/{cluster}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<TaskDetails> getJobTasksById(@PathParam("cluster") String cluster,
                                           @PathParam("jobId") String jobId,
                                           @QueryParam("include") List<String> includeFields,
                                           @QueryParam("includeCounter") List<String>
                                                 includeCounter)
                                               throws IOException {
    LOG.info("Fetching tasks info for jobId=" + jobId);
    Stopwatch timer = new Stopwatch().start();

    Predicate<String> includeFilter = null;
    if (includeFields != null && !includeFields.isEmpty()) {
      includeFilter = new SerializationContext.FieldNameFilter(includeFields);
    }
    Predicate<String> includeCounterFilter = null;
    if (includeCounter != null && !includeCounter.isEmpty()) {
      includeCounterFilter = new SerializationContext.FieldNameFilter(includeCounter);
    }

    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.EVERYTHING, null, null, null,
        includeFilter, includeCounterFilter));

    JobDetails jobDetails = getJobHistoryService().getJobByJobID(cluster,
        jobId, true);
    List<TaskDetails> tasks = jobDetails.getTasks();
    timer.stop();

    if(tasks != null && !tasks.isEmpty()) {
      LOG.info("For endpoint /tasks/" + cluster + "/"
          + jobId + "?"
          + StringUtil.buildParam("include", includeFields)
          + " fetched "
          + tasks.size() + " tasks, spent time " + timer);
    } else {
      LOG.info("For endpoint /tasks/" + cluster + "/" + jobId
          + "?" + StringUtil.buildParam("include", includeFields)
          + ", found no tasks, spent time " + timer);
    }
    return tasks;
  }

  @GET
  @Path("jobFlow/{cluster}/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Flow getJobFlowById(@PathParam("cluster") String cluster,
                             @PathParam("jobId") String jobId,
                             @QueryParam("includeFlowField") List<String> includeFlowFields,
                             @QueryParam("includeJobField") List<String> includeJobFields)
                                 throws IOException {
    LOG.info(String.format("Fetching Flow for cluster=%s, jobId=%s", cluster, jobId));
    Stopwatch timer = new Stopwatch().start();
    Predicate<String> jobFilter = null;
    if (includeJobFields != null && !includeJobFields.isEmpty()) {
      jobFilter = new SerializationContext.FieldNameFilter(includeJobFields);
    }

    Predicate<String> flowFilter = null;
    if (includeFlowFields != null && !includeFlowFields.isEmpty()) {
      flowFilter = new SerializationContext.FieldNameFilter(includeFlowFields);
    }

    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.EVERYTHING, null, flowFilter,
        jobFilter, null));
    Flow flow = getJobHistoryService().getFlowByJobID(cluster, jobId, false);
    timer.stop();

    if (flow != null) {
      LOG.info("For jobFlow/{cluster}/{jobId} with input query: " + "jobFlow/"
          + cluster + SLASH + jobId + "&"
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&"
          + StringUtil.buildParam("includeFlowField", includeFlowFields)
              + " fetched flow " + flow.getFlowName()
          + " with # " + flow.getJobCount() + " in " + timer);
    } else {
      LOG.info("For jobFlow/{cluster}/{jobId} with input query: " + "jobFlow/"
          + cluster + SLASH + jobId + "&"
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&"
          + StringUtil.buildParam("includeFlowField", includeFlowFields)
          + " No flow found, spent " + timer);
    }

    // export latency metrics
    HravenResponseMetrics.JOBFLOW_API_LATENCY_VALUE
        .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return flow;
  }

  @GET
  @Path("flow/{cluster}/{user}/{appId}/{version}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Flow> getJobFlowById(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user,
                                   @PathParam("appId") String appId,
                                   @PathParam("version") String version,
                                   @QueryParam("limit") int limit,
                                   @QueryParam("startTime") long startTime,
                                   @QueryParam("endTime") long endTime,
                                   @QueryParam("include") List<String> include,
                                   @QueryParam("includeConf") List<String> includeConfig,
                                   @QueryParam("includeConfRegex") List<String> includeConfigRegex,
                                   @QueryParam("includeJobField") List<String> includeJobFields)
  throws IOException {

    Stopwatch timer = new Stopwatch().start();

    if(startTime == 0) {
      // look back one month
      startTime = System.currentTimeMillis() - Constants.THIRTY_DAYS_MILLIS;
    }

    if(endTime == 0) {
      // default to now
      endTime = System.currentTimeMillis();
    }

    Predicate<String> configFilter = null;
    if (includeConfig != null && !includeConfig.isEmpty()) {
      configFilter = new SerializationContext.FieldNameFilter(includeConfig);
    } else if (includeConfigRegex != null && !includeConfigRegex.isEmpty()) {
      configFilter = new SerializationContext.RegexConfigurationFilter(
          includeConfigRegex);
    }

    Predicate<String> jobFilter = null;
    if (includeJobFields != null && !includeJobFields.isEmpty()) {
      jobFilter = new SerializationContext.FieldNameFilter(includeJobFields);
    }

    Predicate<String> flowFilter = null;
    if (include != null && !include.isEmpty()) {
      flowFilter = new SerializationContext.FieldNameFilter(include);
    }

    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.EVERYTHING, configFilter, flowFilter,
        jobFilter, null));
    List<Flow> flows = getFlowList(cluster, user, appId, version, startTime,
        endTime, limit);
    timer.stop();

    StringBuilder builderIncludeConfigs = new StringBuilder();
    for(String s : includeConfig) {
      builderIncludeConfigs.append(s);
    }
    StringBuilder builderIncludeConfigRegex = new StringBuilder();
    for(String s : includeConfig) {
      builderIncludeConfigRegex.append(s);
    }

    if (flows != null) {
      LOG.info("For flow/{cluster}/{user}/{appId}/{version} with input query: "
          + "flow/" + cluster
          + SLASH + user + SLASH + appId + SLASH + version
          + "?limit=" + limit
          + " startTime=" + startTime + " endTime=" + endTime
          + " &includeConf=" + builderIncludeConfigs
          + " &includeConfRegex="
          + builderIncludeConfigRegex
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&" + StringUtil.buildParam("include", include)
          + " fetched " + flows.size() + " flows " + " in " + timer);
     } else {
      LOG.info("For flow/{cluster}/{user}/{appId}/{version} with input query: "
          + "flow/" + cluster
          + SLASH + user + SLASH + appId + SLASH + version
          + "?limit=" + limit
          + " startTime=" + startTime + " endTime=" + endTime
          + " &includeConf=" + builderIncludeConfigs
          + "&includeConfRegex=" + builderIncludeConfigRegex
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&" + StringUtil.buildParam("include", include)
          + " No flows fetched, spent " + timer);
    }

    // export latency metrics
    HravenResponseMetrics.FLOW_VERSION_API_LATENCY_VALUE
        .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return flows;
  }

  @GET
  @Path("flow/{cluster}/{user}/{appId}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Flow> getJobFlowById(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user,
                                   @PathParam("appId") String appId,
                                   @QueryParam("limit") int limit,
                                   @QueryParam("startTime") long startTime,
                                   @QueryParam("endTime") long endTime,
                                   @QueryParam("include") List<String> include,
                                   @QueryParam("includeConf") List<String> includeConfig,
                                   @QueryParam("includeConfRegex") List<String> includeConfigRegex,
                                   @QueryParam("includeJobField") List<String> includeJobFields)
                                       throws IOException {

    Stopwatch timer = new Stopwatch().start();
    Predicate<String> configFilter = null;
    if (includeConfig != null && !includeConfig.isEmpty()) {
      configFilter = new SerializationContext.FieldNameFilter(includeConfig);
    } else if (includeConfigRegex != null && !includeConfigRegex.isEmpty()) {
      configFilter = new SerializationContext.RegexConfigurationFilter(
          includeConfigRegex);
    }
    Predicate<String> jobFilter = null;
    if (includeJobFields != null && !includeJobFields.isEmpty()) {
      jobFilter = new SerializationContext.FieldNameFilter(includeJobFields);
    }

    Predicate<String> flowFilter = null;
    if (include != null && !include.isEmpty()) {
      flowFilter = new SerializationContext.FieldNameFilter(include);
    }

    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.EVERYTHING, configFilter, flowFilter,
        jobFilter, null));

    List<Flow> flows = getFlowList(cluster, user, appId, null, startTime,
        endTime, limit);
    timer.stop();

    StringBuilder builderIncludeConfigs = new StringBuilder();
    for(String s : includeConfig) {
      builderIncludeConfigs.append(s);
    }

    StringBuilder builderIncludeConfigRegex = new StringBuilder();
    for(String s : includeConfig) {
      builderIncludeConfigRegex.append(s);
    }

    if (flows != null) {
      LOG.info("For flow/{cluster}/{user}/{appId} with input query: "
          + "flow/" + cluster + SLASH
          + user + SLASH + appId + "?limit=" + limit
          + "&startTime=" + startTime 
          + "&endTime=" + endTime
          + "&includeConf=" + builderIncludeConfigs
          + "&includeConfRegex=" + builderIncludeConfigRegex
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&" + StringUtil.buildParam("include", include)
          + " fetched " + flows.size()
          + " flows in " + timer);
    } else {
      LOG.info("For flow/{cluster}/{user}/{appId} with input query: "
          + "flow/" + cluster + SLASH
          + user + SLASH + appId + "?limit=" + limit
          + "&includeConf=" + builderIncludeConfigs
          + "&includeConfRegex=" + builderIncludeConfigRegex
          + StringUtil.buildParam("includeJobField", includeJobFields)
          + "&" + StringUtil.buildParam("include", include)
          + " No flows fetched, spent "+ timer);
    }

    // export latency metrics
    HravenResponseMetrics.FLOW_API_LATENCY_VALUE
        .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return flows;

  }

  @GET
  @Path("flowStats/{cluster}/{user}/{appId}")
  @Produces(MediaType.APPLICATION_JSON)
  public PaginatedResult<Flow> getJobFlowStats(@PathParam("cluster") String cluster,
                                   @PathParam("user") String user,
                                   @PathParam("appId") String appId,
                                   @QueryParam("version") String version,
                                   @QueryParam("startRow") String startRowParam,
                                   @QueryParam("startTime") long startTime,
                                   @QueryParam("endTime") long endTime,
                                   @QueryParam("limit") @DefaultValue("100") int limit,
                                   @QueryParam("include") List<String> include,
                                   @QueryParam("includeJobs") boolean includeJobs,
                                   @QueryParam("includeJobField") List<String> includeJobFields
                                   ) throws IOException {
    LOG.info("Fetching flowStats for flowStats/{cluster}/{user}/{appId} with input query: "
      + "flowStats/" + cluster  + SLASH + " user " + user
      + appId + "?version=" + version + "&limit=" + limit
      + "&startRow=" + startRowParam + "&startTime=" + startTime
      + "&endTime=" + endTime
      + "&includeJobs=" + includeJobs);

    Stopwatch timer = new Stopwatch().start();
    byte[] startRow = null;
    if (startRowParam != null) {
      startRow = Base64.decode(startRowParam);
    }

    Predicate<String> flowFilter = null;
    if (include != null && !include.isEmpty()) {
      flowFilter = new SerializationContext.FieldNameFilter(include);
    }

    if (includeJobs) {
      Predicate<String> jobFilter = null;
      if (includeJobFields != null && !includeJobFields.isEmpty()) {
        jobFilter = new SerializationContext.FieldNameFilter(includeJobFields);
      }

      serializationContext.set(new SerializationContext(
          SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_WITH_JOB_STATS,
          null, flowFilter, jobFilter, null));
    } else {
      serializationContext.set(new SerializationContext(
          SerializationContext.DetailLevel.FLOW_SUMMARY_STATS_ONLY, null,
          flowFilter, null, null));
    }

    if(endTime == 0) {
      endTime = Long.MAX_VALUE;
    }

    if( (limit == 0) || (limit == Integer.MAX_VALUE)) {
      limit = Integer.MAX_VALUE - 1;
    }

    List<Flow> flows = getJobHistoryService().getFlowTimeSeriesStats(cluster, user,
        appId, version, startTime, endTime, limit + 1, startRow);
    PaginatedResult<Flow> flowStatsPage = new PaginatedResult<Flow>(limit);
    // add request parameters
    flowStatsPage.addRequestParameter("user", user);
    flowStatsPage.addRequestParameter("appId", appId);
    if ( StringUtils.isNotBlank(version)){
      flowStatsPage.addRequestParameter("version", version);
    } else {
      flowStatsPage.addRequestParameter("version", "all");
    }

    flowStatsPage.addRequestParameter("startTime", Long.toString(startTime));
    flowStatsPage.addRequestParameter("endTime", Long.toString(endTime));
    flowStatsPage.addRequestParameter("limit", Integer.toString(limit));

    if ( startRow != null ){
      flowStatsPage.addRequestParameter("startRow", startRowParam);
    }

    if ( includeJobs) {
      flowStatsPage.addRequestParameter("includeJobs", "true");
    } else {
      flowStatsPage.addRequestParameter("includeJobs", "false");
    }

    if (flows.size() > limit) {
      // copy over the last excluding the last element
      // the last element is the start row for next page
      flowStatsPage.setValues(flows.subList(0, limit));
      flowStatsPage.setNextStartRow(new FlowKeyConverter().toBytes(flows.get(limit).getFlowKey()));
    } else {
      flowStatsPage.setNextStartRow(null);
      flowStatsPage.setValues(flows);
    }
    timer.stop();

    LOG.info("For flowStats/{cluster}/{user}/{appId} with input query: "
        + "flowStats/"
        + cluster
        + SLASH // + user /{appId} cluster + " user " + user
        + appId + "?version=" + version + "&limit=" + limit + "&startRow=" + startRow
        + "&startTime=" + startTime + "&endTime=" + endTime + "&includeJobs=" + includeJobs
        + "&" + StringUtil.buildParam("includeJobField", includeJobFields)
        + " fetched " + flows.size() + " in " + timer);

    // export latency metrics
    HravenResponseMetrics.FLOW_STATS_API_LATENCY_VALUE
        .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return flowStatsPage;
 }

   @GET
   @Path("appVersion/{cluster}/{user}/{appId}/")
   @Produces(MediaType.APPLICATION_JSON)
   public List<VersionInfo> getDistinctVersions(@PathParam("cluster") String cluster,
                                    @PathParam("user") String user,
                                    @PathParam("appId") String appId,
                                    @QueryParam("limit") int limit) throws IOException {
     Stopwatch timer = new Stopwatch().start();

     if (LOG.isTraceEnabled()) {
      LOG.trace("Fetching App Versions for cluster=" + cluster + " user=" + user + " app=" + appId);
     }
     serializationContext.set(new SerializationContext(
         SerializationContext.DetailLevel.EVERYTHING));
     List<VersionInfo> distinctVersions = serviceThreadLocalAppVersion.get()
                                             .getDistinctVersions(
                                                 StringUtils.trimToEmpty(cluster),
                                                 StringUtils.trimToEmpty(user),
                                                 StringUtils.trimToEmpty(appId));
     timer.stop();

     LOG.info("For appVersion/{cluster}/{user}/{appId}/ with input query "
       + "appVersion/" + cluster + SLASH + user + SLASH + appId
       + "?limit=" + limit
       + " fetched #number of VersionInfo " + distinctVersions.size() + " in " );//+ timer);

     // export latency metrics
     HravenResponseMetrics.APPVERSIONS_API_LATENCY_VALUE
       .set(timer.elapsed(TimeUnit.MILLISECONDS));
     return distinctVersions;
  }

   @GET
   @Path("getCluster/")
   @Produces(MediaType.APPLICATION_JSON)
   public String getCluster(@QueryParam("hostname") String hostname)
                                        throws ProcessingException, IOException {

     if(StringUtils.isBlank(hostname)) {
       throw new ProcessingException("hostname not present, "
         + "please resend with that info");
     }
    String cluster = Cluster.getIdentifier(hostname);
    LOG.info("Fetched cluster identifier=" + cluster + " for hostname=" + hostname);
    return cluster;
   }

  private List<Flow> getFlowList(String cluster,
                                 String user,
                                 String appId,
                                 String version,
                                 long startTime,
                                 long endTime,
                                 int limit) throws IOException {
    if (limit < 1) {
      limit = 1;
    }
    LOG.info(String.format(
      "Fetching Flow series for cluster=%s, user=%s, appId=%s, version=%s, limit=%s", cluster,
      user, appId, version, limit));

    List<Flow> flows =
        getJobHistoryService().getFlowSeries(cluster, user, appId, version, false, startTime,
          endTime, limit);
    LOG.info(String.format("Found %s flows", flows.size()));
    return flows;
  }

  @GET
  @Path("hdfs/{cluster}/")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HdfsStats> getHdfsStats(@PathParam("cluster") String cluster,
                                // run Id is timestamp in seconds
                                @QueryParam("timestamp") long runid,
                                @QueryParam("path") String pathPrefix,
                                @QueryParam("limit") int limit)
                                    throws IOException {
    if (limit == 0) {
      limit = HdfsConstants.RECORDS_RETURNED_LIMIT;
    }

    boolean noRunId = false;
    if (runid == 0L) {
      // default it to 2 hours back
      long lastHour = System.currentTimeMillis() - 2 * 3600000L;
      // convert milliseconds to seconds
      runid = lastHour / 1000L;
      noRunId = true;
    }

    LOG.info(String.format("Fetching hdfs stats for cluster=%s, path=%s limit=%d, runId=%d",
      cluster, pathPrefix, limit, runid));
    Stopwatch timer = new Stopwatch().start();
    serializationContext.set(new SerializationContext(SerializationContext.DetailLevel.EVERYTHING));
    List<HdfsStats> hdfsStats = getHdfsStatsService().getAllDirs(cluster, pathPrefix, limit, runid);
    timer.stop();
    /**
     * if we find NO hdfs stats for the default timestamp
     * consider the case when no runId is passed
     * in that means user is expecting a default response
     * we set the default runId to 2 hours back
     * as above but what if there was an error in
     * collection at that time? hence we try to look back
     * for some older runIds
     */
    if (hdfsStats == null || hdfsStats.size() == 0L) {
      if (noRunId) {
        // consider reading the daily aggregation table instead of hourly
        // or consider reading older data since runId was a default timestamp
        int retryCount = 0;
        while (retryCount < HdfsConstants.ageMult.length) {
          runid = HdfsStatsService.getOlderRunId(retryCount, runid);
          hdfsStats = getHdfsStatsService().getAllDirs(cluster, pathPrefix, limit, runid);
          if ((hdfsStats != null) && (hdfsStats.size() != 0L)) {
            break;
          }
          retryCount++;
        }
      }
  }

    // export latency metrics
    HravenResponseMetrics.HDFS_STATS_API_LATENCY_VALUE
      .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return hdfsStats;
  }

  @GET
  @Path("hdfs/path/{cluster}/")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HdfsStats> getHdfsPathTimeSeriesStats(
                                @PathParam("cluster") String cluster,
                                @QueryParam("path") String path,
                                @QueryParam("starttime") long starttime,
                                @QueryParam("endtime") long endtime,
                                @QueryParam("limit") int limit)
                                    throws IOException {
    if( StringUtils.isEmpty(path)) {
      throw new RuntimeException("Required query param missing: path ");
    }

    if (limit == 0) {
      limit = HdfsConstants.RECORDS_RETURNED_LIMIT;
    }

    if (starttime == 0L) {
      // default it to current hour's top
      long lastHour = System.currentTimeMillis();
      // convert milliseconds to seconds
      starttime = lastHour / 1000L;
    }

    if (endtime == 0L) {
      // default it to one week ago
      endtime = starttime - 7 * 86400;
    }

    if(endtime > starttime) {
      throw new RuntimeException("Ensure endtime " + endtime + " is older than starttime " + starttime);
    }

    LOG.info(String.format(
      "Fetching hdfs timeseries stats for cluster=%s, path=%s limit=%d, starttime=%d endtime=%d", cluster,
      path, limit, starttime, endtime));
    Stopwatch timer = new Stopwatch().start();
    List<HdfsStats> hdfsStats =
        getHdfsStatsService().getHdfsTimeSeriesStats(cluster, path, limit, starttime,
          endtime);
    timer.stop();

    if( hdfsStats != null ){
    LOG.info("For hdfs/path/{cluster}/{attribute} with input query "
        +  "hdfs/path/" + cluster
        + "?limit=" + limit + "&path=" + path
        + " fetched #number of HdfsStats " + hdfsStats.size() + " in " + timer);
    } else {
      LOG.info("For hdfs/path/{cluster}/{attribute} with input query "
          +  "hdfs/path/" + cluster
          + "?limit=" + limit + "&path=" + path
          + " fetched 0 HdfsStats in " + timer);
    }

    // export latency metrics
    HravenResponseMetrics.HDFS_TIMESERIES_API_LATENCY_VALUE
       .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return hdfsStats;
  }

  @GET
  @Path("newJobs/{cluster}/")
  @Produces(MediaType.APPLICATION_JSON)
  public List<AppSummary> getNewJobs(@PathParam("cluster") String cluster,
                                   @QueryParam("user") String user,
                                   @QueryParam("startTime") long startTime,
                                   @QueryParam("endTime") long endTime,
                                   @QueryParam("limit") int limit)
                                       throws IOException {
    Stopwatch timer = new Stopwatch().start();

    if(limit == 0) {
      limit = Integer.MAX_VALUE;
    }
    if(startTime == 0L) {
      // 24 hours back
      startTime = System.currentTimeMillis() - Constants.MILLIS_ONE_DAY ;
      // get top of the hour
      startTime -= (startTime % 3600);
    }
    if(endTime == 0L) {
      // now
      endTime = System.currentTimeMillis() ;
      // get top of the hour
      endTime -= (endTime % 3600);
    }

    LOG.info("Fetching new Jobs for cluster=" + cluster + " user=" + user
       + " startTime=" + startTime + " endTime=" + endTime);
    AppSummaryService as = getAppSummaryService();
    // get the row keys from AppVersions table via JobHistoryService
    List<AppSummary> newApps = as.getNewApps(getJobHistoryService(),
          StringUtils.trimToEmpty(cluster), StringUtils.trimToEmpty(user),
          startTime, endTime, limit);

    timer.stop();

    LOG.info("For newJobs/{cluster}/{user}/{appId}/ with input query "
      + "newJobs/" + cluster + SLASH + user
      + "?limit=" + limit
      + "&startTime=" + startTime
      + "&endTime=" + endTime
      + " fetched " + newApps.size() + " flows in " + timer);

   serializationContext.set(new SerializationContext(
      SerializationContext.DetailLevel.APP_SUMMARY_STATS_NEW_JOBS_ONLY));

   // export latency metrics
   HravenResponseMetrics.NEW_JOBS_API_LATENCY_VALUE
       .set(timer.elapsed(TimeUnit.MILLISECONDS));
    return newApps;
 }

  @GET
  @Path("summary/apps/{cluster}/")
  @Produces(MediaType.APPLICATION_JSON)
  public List<AppSummary> getAllApps(@PathParam("cluster") String cluster,
                                     @QueryParam("user") String user,
                                     @QueryParam("startTime") long startTime,
                                     @QueryParam("endTime") long endTime,
                                     @QueryParam("limit") int limit)
                                         throws IOException {
    Stopwatch timer = new Stopwatch().start();

    if (limit == 0) {
      limit = Integer.MAX_VALUE;
    }
    if (startTime == 0L) {
      // 24 hours back
      startTime = System.currentTimeMillis() - Constants.MILLIS_ONE_DAY;
      // get top of the hour
      startTime -= (startTime % 3600000);
    }
    if (endTime == 0L) {
      // now
      endTime = System.currentTimeMillis();
      // get top of the hour
      endTime -= (endTime % 3600000);
    }

    LOG.info("Fetching all apps for cluster=" + cluster + " user=" + user + " startTime="
        + startTime + " endTime=" + endTime);
    AppSummaryService as = getAppSummaryService();
    List<AppSummary> newApps = as.getAllApps(
          StringUtils.trimToEmpty(cluster),
          StringUtils.trimToEmpty(user),
          startTime,
          endTime,
          limit);
    timer.stop();
    LOG.info("For summary/apps/{cluster}/{user}/{appId}/ with input query " + "summary/apps/"
        + cluster + SLASH + user + "?limit=" + limit + "&startTime=" + startTime + "&endTime="
        + endTime + " fetched " + newApps.size() + " apps in " + timer);
    serializationContext.set(new SerializationContext(
        SerializationContext.DetailLevel.APP_SUMMARY_STATS_ALL_APPS));
    return newApps;
  }

  private AppSummaryService getAppSummaryService() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning AppService %s bound to thread %s",
        serviceThreadLocalAppService.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocalAppService.get();
  }

  private HdfsStatsService getHdfsStatsService() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning HdfsStats %s bound to thread %s",
        serviceThreadLocalHdfsStats.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocalHdfsStats.get();
  }

  private static JobHistoryService getJobHistoryService() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returning JobHistoryService %s bound to thread %s",
        serviceThreadLocal.get(), Thread.currentThread().getName()));
    }
    return serviceThreadLocal.get();
  }
}
