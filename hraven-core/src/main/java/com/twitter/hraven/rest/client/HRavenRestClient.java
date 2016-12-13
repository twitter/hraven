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
package com.twitter.hraven.rest.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.hraven.Flow;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.TaskDetails;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.rest.ObjectMapperProvider;
import com.twitter.hraven.rest.RestJSONResource;
import com.twitter.hraven.rest.ObjectMapperProvider.FlowSerializer;
import com.twitter.hraven.rest.ObjectMapperProvider.JobDetailsSerializer;
import com.twitter.hraven.rest.ObjectMapperProvider.TaskDetailsSerializer;
import com.twitter.hraven.util.StringUtil;

/**
 * Java REST client class for fetching from rest server
 *
 */
public class HRavenRestClient {
  private static final Log LOG = LogFactory.getLog(HRavenRestClient.class);

  private String apiHostname;
  private int connectTimeout;
  private int readTimeout;

  public static final String URL_PORTION_API_V1 = "api/v1/";
  public static final String AND = "&";
  public static final String QUESTION_MARK = "?";
  public static final String LIMIT = "limit";
  public static final String FLOW_API = "flow";
  public static final String EQUAL_TO = "=";

  /**
   * Initializes with the given hostname and a default connect and read timeout of 5 seconds.
   * @param apiHostname the hostname to connect to
   */
  public HRavenRestClient(String apiHostname) {
    this(apiHostname, 5000, 5000);
  }

  public HRavenRestClient(String apiHostname, int connectTimeout, int readTimeout) {
    this.apiHostname = apiHostname;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    LOG.info(String.format(
      "Initializing HRavenRestClient with apiHostname=%s, connectTimeout=%d ms, readTimeout=%d ms",
      apiHostname, connectTimeout, readTimeout));
  }

  public String getCluster(String hostname) throws IOException {
    String urlString =
        String.format("http://%s/api/v1/getCluster?hostname=%s", apiHostname,
          StringUtil.cleanseToken(hostname));

    if (LOG.isInfoEnabled()) {
      LOG.info("Requesting cluster for " + hostname);
    }
    URL url = new URL(urlString);
    URLConnection connection = url.openConnection();
    connection.setConnectTimeout(this.connectTimeout);
    connection.setReadTimeout(this.readTimeout);
    InputStream input = connection.getInputStream();
    java.util.Scanner s = new java.util.Scanner(input).useDelimiter("\\A");
    String cluster = s.hasNext() ? s.next() : "";
    try {
      input.close();
    } catch (IOException ioe) {
      LOG.error("IOException in closing input stream, returning no error "
          + ioe.getMessage());
    }
    return cluster;
  }

  public List<Flow> fetchFlows(String cluster,
                               String username,
                               String batchDesc,
                               String signature,
                               int limit) throws IOException {
    LOG.info(String.format("Fetching last %d matching jobs for cluster=%s, user.name=%s, " +
      "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster, username, batchDesc, signature)); 

    String urlString = signature == null ?
      String.format("http://%s/api/v1/flow/%s/%s/%s?limit=%d",
        apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), limit) :
      String.format("http://%s/api/v1/flow/%s/%s/%s/%s?limit=%d",
        apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), signature, limit);

    return retrieveFlowsFromURL(urlString);
  }

  public List<Flow> fetchFlows(String cluster,
                               String username,
                               String batchDesc,
                               String signature,
                               List<String> flowResponseFilters,
                               List<String> jobResponseFilters,
                               int limit) throws IOException {
    LOG.info(String.format(
        "Fetching last %d matching jobs for cluster=%s, user.name=%s, "
            + "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster,
        username, batchDesc, signature));

    StringBuilder urlStringBuilder = buildFlowURL(cluster, username, batchDesc,
        signature, limit, flowResponseFilters, jobResponseFilters);

    return retrieveFlowsFromURL(urlStringBuilder.toString());
  }

  /**
   * Fetches a list of flows that include jobs in that flow that include the
   * specified configuration properties
   * 
   * @param cluster
   * @param username
   * @param batchDesc
   * @param signature
   * @param limit
   * @param configProps
   * @return list of flows
   * @throws IOException
   */
  public List<Flow> fetchFlowsWithConfig(String cluster,
                                         String username,
                                         String batchDesc,
                                         String signature,
                                         int limit,
                                         String... configProps) throws IOException {
    LOG.info(String.format("Fetching last %d matching jobs for cluster=%s, user.name=%s, " +
        "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster, username, batchDesc, signature));

    String configParam = "";
    if (configProps != null && configProps.length > 0) {
      configParam = StringUtil.buildParam("includeConf", configProps);
    }
    String urlString = signature == null ?
        String.format("http://%s/api/v1/flow/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), limit, configParam) :
        String.format("http://%s/api/v1/flow/%s/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), signature, limit,
            configParam);

    return retrieveFlowsFromURL(urlString);
  }

  /**
   * Fetches a list of flows that include jobs in that flow that include the
   * specified flow fields and job fields
   * specified configuration properties
   * @param cluster
   * @param username
   * @param batchDesc
   * @param signature
   * @param limit
   * @param flowResponseFilters
   * @param jobResponseFilters
   * @param configPropertyFields
   * @return list of flows
   * @throws IOException
   */
  public List<Flow> fetchFlowsWithConfig(String cluster,
                                         String username,
                                         String batchDesc,
                                         String signature,
                                         int limit,
                                         List<String> flowResponseFilters,
                                         List<String> jobResponseFilters,
                                         List<String> configPropertyFields) throws IOException {
    LOG.info(String.format("Fetching last %d matching jobs for cluster=%s, user.name=%s, " +
        "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster, username, batchDesc, signature));
    StringBuilder urlStringBuilder = buildFlowURL(cluster, username, batchDesc,
        signature, limit, flowResponseFilters, jobResponseFilters);

    if ((configPropertyFields != null) && (configPropertyFields.size() > 0)) {
      urlStringBuilder.append(AND);
      urlStringBuilder.append(StringUtil.buildParam("includeConf",
           configPropertyFields));
    }
    return retrieveFlowsFromURL(urlStringBuilder.toString());
  }

  /**
   * Returns a list of flows that contain config elements
   * matching a specific pattern.
   * @param cluster
   * @param username
   * @param batchDesc
   * @param signature
   * @param limit
   * @param configPatterns
   * @return
   * @throws IOException
   */
  public List<Flow> fetchFlowsWithConfigPatterns(String cluster,
                                         String username,
                                         String batchDesc,
                                         String signature,
                                         int limit,
                                         String... configPatterns) throws IOException {
    LOG.info(String.format("Fetching last %d matching jobs for cluster=%s, user.name=%s, " +
        "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster, username, batchDesc, signature));

    String configParam = "";
    if (configPatterns != null && configPatterns.length > 0) {
      configParam = StringUtil.buildParam("includeConfRegex", configPatterns);
    }
    String urlString = signature == null ?
        String.format("http://%s/api/v1/flow/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), limit, configParam) :
        String.format("http://%s/api/v1/flow/%s/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), signature, limit,
            configParam);

    return retrieveFlowsFromURL(urlString);
  }

  public List<Flow> fetchFlowsWithConfigPatterns(String cluster,
                                                 String username,
                                                 String batchDesc,
                                                 String signature,
                                                 int limit,
                                                 List<String> flowResponseFilters,
                                                 List<String> jobResponseFilters,
                                                 List<String> configPatterns)
                                                     throws IOException {
    LOG.info(String.format(
        "Fetching last %d matching jobs for cluster=%s, user.name=%s, "
            + "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster,
        username, batchDesc, signature));

    StringBuilder urlStringBuilder = buildFlowURL(cluster, username, batchDesc,
        signature, limit, flowResponseFilters, jobResponseFilters);

    if ((configPatterns != null) && (configPatterns.size() > 0)) {
      urlStringBuilder.append(AND);
      urlStringBuilder.append(StringUtil.buildParam("includeConfRegex",
           configPatterns));
    }
    return retrieveFlowsFromURL(urlStringBuilder.toString());
  }

  /**
   * builds up a StringBuilder with the parameters for the FLOW API
   * @param cluster
   * @param username
   * @param batchDesc
   * @param signature
   * @param limit
   * @param flowResponseFilters
   * @param jobResponseFilters
   * @return
   * @throws IOException
   */
  private StringBuilder buildFlowURL(String cluster,
      String username,
      String batchDesc,
      String signature,
      int limit,
      List<String> flowResponseFilters,
      List<String> jobResponseFilters) throws IOException {
    StringBuilder urlStringBuilder = new StringBuilder();
    urlStringBuilder.append("http://");
    urlStringBuilder.append(apiHostname);
    urlStringBuilder.append(RestJSONResource.SLASH);
    urlStringBuilder.append(URL_PORTION_API_V1);
    urlStringBuilder.append(FLOW_API);
    urlStringBuilder.append(RestJSONResource.SLASH);
    urlStringBuilder.append(cluster);
    urlStringBuilder.append(RestJSONResource.SLASH);
    urlStringBuilder.append(username);
    urlStringBuilder.append(RestJSONResource.SLASH);
    urlStringBuilder.append(StringUtil.cleanseToken(batchDesc));
    if (StringUtils.isNotEmpty(signature)) {
      urlStringBuilder.append(RestJSONResource.SLASH);
      urlStringBuilder.append(signature);
    }
    urlStringBuilder.append(QUESTION_MARK);
    urlStringBuilder.append(LIMIT);
    urlStringBuilder.append(EQUAL_TO);
    urlStringBuilder.append(limit);
    if ((flowResponseFilters != null) && (flowResponseFilters.size() > 0)) {
      urlStringBuilder.append(AND);
      urlStringBuilder.append(StringUtil.buildParam("include",
          flowResponseFilters));
    }

    if ((jobResponseFilters != null) && (jobResponseFilters.size() > 0)) {
      urlStringBuilder.append(AND);
      urlStringBuilder.append(StringUtil.buildParam("includeJobField",
          jobResponseFilters));
    }

    return urlStringBuilder;
  }

  private List<Flow> retrieveFlowsFromURL(String endpointURL) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Requesting job history from " + endpointURL);
    }
    return new UrlDataLoader<Flow>(
        endpointURL,
        new TypeReference<List<Flow>>() {},
        connectTimeout,
        readTimeout).load();
  }

  /**
   * Fetch details tasks of a given job.
   * @param cluster
   * @param jobId
   * @return
   */
  public List<TaskDetails> fetchTaskDetails(String cluster, String jobId) throws IOException {
    String urlString = String.format("http://%s/api/v1/tasks/%s/%s", apiHostname, cluster, jobId);
    return retrieveTaskDetailsFromUrl(urlString);
  }

  /**
   * Fetch details tasks of a given job for the specified fields
   * @param cluster
   * @param jobId
   * @param taskResponseFilters
   * @return
   */
  public List<TaskDetails> fetchTaskDetails(String cluster, String jobId,
      List<String> taskResponseFilters) throws IOException {
    String taskFilters = StringUtil.buildParam("include",
        taskResponseFilters);
    String urlString = String.format("http://%s/api/v1/tasks/%s/%s?%s",
        apiHostname, cluster, jobId, taskFilters);
    return retrieveTaskDetailsFromUrl(urlString);
  }

  /**
   * Fetch details tasks of a given job for the specified fields
   * @param cluster
   * @param jobId
   * @param taskResponseFilters
   * @return
   */
  public List<TaskDetails> fetchTaskDetails(String cluster, String jobId,
                                            List<String> taskResponseFilters,
                                            List<String>
                                                taskResponseCounterFilters)
      throws IOException {
    String taskFilters = StringUtil.buildParam("include",
        taskResponseFilters);
    String taskCounterFilters = StringUtil.buildParam("includeCounter",
        taskResponseCounterFilters);

    String urlString = String.format("http://%s/api/v1/tasks/%s/%s?%s&%s",
        apiHostname, cluster, jobId, taskFilters, taskCounterFilters);
    System.out.println("------------------- " + urlString);
    return retrieveTaskDetailsFromUrl(urlString);
  }

  private List<TaskDetails> retrieveTaskDetailsFromUrl(String endpointURL)
      throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Requesting task history from " + endpointURL);
    }
    return new UrlDataLoader<TaskDetails>(
        endpointURL,
        new TypeReference<List<TaskDetails>>() {},
        connectTimeout,
        readTimeout).load();
  }

  private static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws IOException {
    String apiHostname = null;
    String cluster =  null;
    String username = null;
    String batchDesc = null;
    String signature = null;
    int limit = 2;
    boolean useHBaseAPI = false;
    boolean dumpJson = true;
    boolean hydrateTasks = true;
    List<String> taskResponseFilters = new ArrayList<String>();
    List<String> taskCounterResponseFilters = new ArrayList<String>();
    List<String> jobResponseFilters = new ArrayList<String>();
    List<String> flowResponseFilters = new ArrayList<String>();
    List<String> configFields = new ArrayList<String>();

    StringBuffer usage = new StringBuffer("Usage: java ");
    usage.append(HRavenRestClient.class.getName()).append(" [-options]\n");
    usage.append("Returns data from recent flows and their associated jobs\n");
    usage.append("where options include: \n");
    usage.append(" -a <API hostname> [required]\n");
    usage.append(" -c <cluster> [required]\n");
    usage.append(" -u <username> [required]\n");
    usage.append(" -f <flowName> [required]\n");
    usage.append(" -s <signature>\n");
    usage.append(" -l <limit>\n");
    usage.append(" -h - print this message and return\n");
    usage.append(" -H - use HBase API, not the REST API\n");
    usage.append(" -j - output json\n");
    usage.append(" -t - retrieve task information as well");
    usage.append(" -w - config field to be included in job response");
    usage.append(" -z - field to be included in task response");
    usage.append(" -q - counter to be included in task response");
    usage.append(" -y - field to be included in job response");
    usage.append(" -x - field to be included in flow response");

    for (int i = 0; i < args.length; i++) {
      if("-a".equals(args[i])) {
        apiHostname = args[++i];
        continue;
      } else if("-c".equals(args[i])) {
        cluster = args[++i];
        continue;
      } else if("-u".equals(args[i])) {
        username = args[++i];
        continue;
      } else if("-f".equals(args[i])) {
        batchDesc = args[++i];
        continue;
      } else if("-s".equals(args[i])) {
        signature = args[++i];
        continue;
      } else if("-l".equals(args[i])) {
        limit = Integer.parseInt(args[++i]);
        continue;
      } else if("-H".equals(args[i])) {
        useHBaseAPI = true;
        continue;
      } else if("-j".equals(args[i])) {
        dumpJson = true;
        continue;
      } else if("-t".equals(args[i])) {
        hydrateTasks = true;
        continue;
      } else if("-z".equals(args[i])) {
        String taskFilters =  args[++i];
        taskResponseFilters = Arrays.asList(taskFilters.split(","));
        System.out.println("************* task filters = " + taskResponseFilters.toString());
        continue;
      } else if("-q".equals(args[i])) {
        String taskCounterFilters =  args[++i];
        taskCounterResponseFilters = Arrays.asList(taskCounterFilters.split(","));
        continue;
      } else if("-y".equals(args[i])) {
        String jobFilters =  args[++i];
        jobResponseFilters = Arrays.asList(jobFilters.split(","));
        continue;
      } else if("-x".equals(args[i])) {
        String flowFilters =  args[++i];
        flowResponseFilters =Arrays.asList(flowFilters.split(","));
        continue;
      } else if("-w".equals(args[i])) {
        String configFilters =  args[++i];
        configFields =Arrays.asList(configFilters.split(","));
        continue;
      } else if ("-h".equals(args[i])) {
        System.err.println(usage.toString());
        System.exit(1);
      } else {

      }
    }

    if (apiHostname == null || cluster == null || username == null || batchDesc == null) {
      System.err.println(usage.toString());
      System.exit(1);
    }

    System.out.println(" here **** ");
    List<Flow> flows;
    if (useHBaseAPI) {
      JobHistoryService jobHistoryService = new JobHistoryService(HBaseConfiguration.create());
      flows = jobHistoryService.getFlowSeries(cluster, username, batchDesc,
          signature, hydrateTasks, limit);
    } else {
      HRavenRestClient client = new HRavenRestClient(apiHostname, 100000, 100000);

      // use this call to call flows without configs
      flows = client.fetchFlows(cluster, username, batchDesc, signature,
          flowResponseFilters, jobResponseFilters, limit);
      // use this call to call flows with configs
    //  flows = client.fetchFlowsWithConfig(cluster, username, batchDesc, signature,
     //    limit, flowResponseFilters, jobResponseFilters, configFields );
      // use this call to call flows with config patterns
       //   flows = client.fetchFlowsWithConfig(cluster, username, batchDesc, signature,
      //        limit, flowResponseFilters, jobResponseFilters, configFields );

      if (hydrateTasks) {
        for (Flow flow : flows) {
          for (JobDetails jd : flow.getJobs()) {
            String jobId = jd.getJobId();
            List<TaskDetails> td = client.fetchTaskDetails(cluster, jobId, taskResponseFilters,
                taskCounterResponseFilters);
            for(TaskDetails t1: td) {
              if (t1.getCounters() != null &&
                  t1.getCounters().getCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMMITTED_HEAP_BYTES") != null) {
              System.out.println("task details type:" + t1.getTaskKey().toString() +
                  " " + t1.getType()
                 // + " " + t1.getCounters().getGroup("org.apache.hadoop.mapreduce.TaskCounter").size()
                  + " " 
                  + t1.getCounters()
                  .getCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMMITTED_HEAP_BYTES").getKey()
                  + " " 
                  + t1.getCounters()
                  .getCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMMITTED_HEAP_BYTES").getValue());
              }
            }
            jd.addTasks(td);
          }
        }
      }
    }

    dumpJson = false;
    if (dumpJson) {
      ObjectMapper om = ObjectMapperProvider.createCustomMapper();
      SimpleModule module = new SimpleModule("hRavenModule", new Version(0, 4,
          0, null));
      module.addSerializer(Flow.class, new FlowSerializer());
      module.addSerializer(JobDetails.class, new JobDetailsSerializer());
      module.addSerializer(TaskDetails.class, new TaskDetailsSerializer());
      om.registerModule(module);
      if (flows.size() > 0) {
//        System.out.println(om.writeValueAsString(flows.get(0)));
        System.out.println(om.writeValueAsString(flows.get(0)));

      }
      return;
    }

    System.out.println("Found " + flows.size() + " flows");
    StringBuilder sb = new StringBuilder();
    sb.append("\t\t").append("jobId");
    sb.append("\t\t").append("version");
    sb.append("\t\t").append("status");
    sb.append("\t").append("maps");
    sb.append("\t").append("reduces");
    sb.append("\t").append("rBytesRead");
    sb.append("\t").append("feature");
    sb.append("\t\t\t").append("alias");
    System.out.println(sb.toString());

    int i = 0;
    for (Flow flow : flows) {
      long minSubmitTime = -1, maxFinishTime = -1;
      for (JobDetails job : flow.getJobs()) {
        if (minSubmitTime == -1 && job.getSubmitTime() > 0) {
          minSubmitTime = job.getSubmitTime();
        }
        minSubmitTime = Math.min(minSubmitTime, job.getSubmitTime());
        maxFinishTime = Math.max(maxFinishTime, job.getFinishTime());
      }

      if (minSubmitTime > 0 && maxFinishTime > 0) {
        System.out.println(String.format("Flow #%d: %s - %s", i++,
          DATE_FORMAT.format(minSubmitTime), DATE_FORMAT.format(maxFinishTime)));
      } else {
        System.out.println(String.format("Flow #%d:", i++));
      }

      for (JobDetails job : flow.getJobs()) {
        sb = new StringBuilder();
        sb.append(" - ").append(job.getJobId());
        sb.append("\t").append(job.getVersion());
        sb.append("\t").append(job.getStatus());
        sb.append("\t").append(job.getTotalMaps());
        sb.append("\t").append(job.getTotalReduces());
        long reduceBytesRead = job.getReduceCounters().getCounter("FileSystemCounters", "FILE_BYTES_READ") != null ?
          job.getReduceCounters().getCounter("FileSystemCounters", "FILE_BYTES_READ").getValue() : -1;
        sb.append("\t").append(reduceBytesRead);
        sb.append("\t").append(job.getConfiguration().get("pig.job.feature"));
        sb.append("\t").append(job.getConfiguration().get("pig.alias"));
        System.out.println(sb.toString());
      }
    }
  }
}