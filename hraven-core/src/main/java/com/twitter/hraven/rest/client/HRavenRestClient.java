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
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.hraven.Flow;
import com.twitter.hraven.JobDetails;
import com.twitter.hraven.TaskDetails;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.rest.ObjectMapperProvider;
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
      configParam = buildConfigParam("includeConf", configProps);
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
                                         String... configPatterns) throws IOException {
    LOG.info(String.format("Fetching last %d matching jobs for cluster=%s, user.name=%s, " +
        "batch.desc=%s, pig.logical.plan.signature=%s", limit, cluster, username, batchDesc, signature));

    String configParam = "";
    if (configPatterns != null && configPatterns.length > 0) {
      configParam = buildConfigParam("includeConfRegex", configPatterns);
    }
    String urlString = signature == null ?
        String.format("http://%s/api/v1/flow/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), limit, configParam) :
        String.format("http://%s/api/v1/flow/%s/%s/%s/%s?limit=%d&%s",
            apiHostname, cluster, username, StringUtil.cleanseToken(batchDesc), signature, limit,
            configParam);

    return retrieveFlowsFromURL(urlString);
  }

  private String buildConfigParam(String paramName, String[] paramArgs) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (String arg : paramArgs) {
      if (sb.length() > 0) {
        sb.append("&");
      }
      sb.append(paramName).append("=").append(URLEncoder.encode(arg, "UTF-8"));
    }
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  private List<TaskDetails> retrieveTaskDetailsFromUrl(String endpointURL) throws IOException {
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
    String cluster = null;
    String username = null;
    String batchDesc = null;
    String signature = null;
    int limit = 2;
    boolean useHBaseAPI = false;
    boolean dumpJson = false;
    boolean hydrateTasks = false;

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

    List<Flow> flows;
    if (useHBaseAPI) {
      JobHistoryService jobHistoryService = new JobHistoryService(HBaseConfiguration.create());
      flows = jobHistoryService.getFlowSeries(cluster, username, batchDesc,
          signature, hydrateTasks, limit);
    } else {
      HRavenRestClient client = new HRavenRestClient(apiHostname);
      flows = client.fetchFlows(cluster, username, batchDesc, signature, limit);
      if (hydrateTasks) {
        for (Flow flow : flows) {
          for (JobDetails jd : flow.getJobs()) {
            String jobId = jd.getJobId();
            List<TaskDetails> td = client.fetchTaskDetails(cluster, jobId);
            jd.addTasks(td);
          }
        }
      }
    }

    if(dumpJson) {
      ObjectMapper om = ObjectMapperProvider.createCustomMapper();
      System.out.println(om.writeValueAsString(flows));
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