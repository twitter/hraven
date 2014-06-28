package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.kenai.jffi.Array;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Framework;
import com.twitter.hraven.HravenService;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;

public class GraphiteHistoryWriter {

  private static Log LOG = LogFactory.getLog(GraphiteHistoryWriter.class);

  private final Pattern APPID_PATTERN_OOZIE_LAUNCHER = Pattern.compile("oozie:launcher:T=(.*):W=(.*):A=(.*):ID=(.*)");
  private final Pattern APPID_PATTERN_OOZIE_ACTION = Pattern.compile("oozie:action:T=(.*):W=(.*):A=(.*):ID=[0-9]{7}-[0-9]{15}-oozie-oozi-W(.*)");
  private final Pattern APPID_PATTERN_PIGJOB = Pattern.compile("PigLatin:(.*).pig");
  private static final String GRAPHITE_KEY_FILTER = "[./\\\\\\s,]";
  private static final int PIG_ALIAS_FINGERPRINT_LENGTH = 100;
  
  private static final String submitTimeKey = JobHistoryKeys.SUBMIT_TIME.toString();
  private static final String launchTimeKey = JobHistoryKeys.LAUNCH_TIME.toString();
  private static final String finishTimeKey = JobHistoryKeys.FINISH_TIME.toString();
  private static final String totalTimeKey = "total_time";
  private static final String runTimeKey = "run_time";

  private HravenService service;
  private JobHistoryRecordCollection recordCollection;
  private String prefix;
  private StringBuilder lines;
  private List<String> userFilter;
  private List<String> queueFilter;
  private List<String> excludedComponents;
  private List<String> doNotExcludeApps;
  
  private HTable keyMappingTable;
  private HTable reverseKeyMappingTable;

  /**
   * Writes a single {@link JobHistoryRecord} to the specified {@link HravenService} Passes the
   * large multi record of which this record is a part of, so that we can get other contextual
   * attributes to use in the graphite metric naming scheme
   * @param graphiteKeyMappingTable 
   * @param serviceKey
   * @param userFilter 
   * @param doNotExcludeApps 
   * @param jobRecord
   * @param multiRecord
   * @throws IOException
   * @throws InterruptedException
   */

  public GraphiteHistoryWriter(HTable keyMappingTable, HTable reverseKeyMappingTable, String prefix, HravenService serviceKey,
      JobHistoryRecordCollection recordCollection, StringBuilder sb, String userFilter, String queueFilter, String excludedComponents, String doNotExcludeApps) {
    this.keyMappingTable = keyMappingTable;
    this.reverseKeyMappingTable = reverseKeyMappingTable;
    this.service = serviceKey;
    this.recordCollection = recordCollection;
    this.prefix = prefix;
    this.lines = sb;
    if (StringUtils.isNotEmpty(userFilter))
      this.userFilter = Arrays.asList(userFilter.split(","));
    if (StringUtils.isNotEmpty(queueFilter))
      this.queueFilter = Arrays.asList(queueFilter.split(","));
    if (StringUtils.isNotEmpty(excludedComponents))
      this.excludedComponents = Arrays.asList(excludedComponents.split(","));
    if (StringUtils.isNotEmpty(doNotExcludeApps))
      this.doNotExcludeApps = Arrays.asList(doNotExcludeApps.split(","));
  }

  public int write() throws IOException {
    /*
     * Send metrics in the format {PREFIX}.{cluster}.{user}.{appId}.{subAppId} {value}
     * {submit_time} subAppId is formed differently for each framework. For pig, its the alias
     * names and feature used in the job. appId will be parsed with a bunch of known patterns
     * (oozie launcher jobs, pig jobs, etc.)
     */

    int lineCount = 0;
    
    boolean inDoNotExcludeApps = false;
    
    if (doNotExcludeApps != null) {
      for (String appStr: this.doNotExcludeApps) {
        if (recordCollection.getKey().getAppId().indexOf(appStr) != -1) {
          inDoNotExcludeApps = true;
          break;
        }
      }  
    }
    
    if ( 
        // exclude from further filters if appId matches list of doNotExcludeApps substrings
        inDoNotExcludeApps ||
        
        // or it must pass the user and queue filters, if not null 
        ( (userFilter == null || userFilter.contains(recordCollection.getKey().getUserName())) &&
         (queueFilter == null || queueFilter.contains(recordCollection.getValue(RecordCategory.CONF_META,
                                                                           new RecordDataKey(Constants.HRAVEN_QUEUE)))) )
      ) {
      
      Framework framework = getFramework(recordCollection);
      String metricsPathPrefix;

      String pigAliasFp = getPigAliasFingerprint(recordCollection);
      String genAppId = genAppId(recordCollection, recordCollection.getKey().getAppId());
      
      if (genAppId == null) {
        genAppId = recordCollection.getKey().getAppId();
        LOG.error("Generated appId is null for app " + recordCollection.getKey().toString());
      }
      
      if (framework == Framework.PIG && pigAliasFp != null) {
        // TODO: should ideally include app version too, but PIG-2587's pig.logical.plan.signature
        // which hraven uses was available only from pig 0.11
        
        metricsPathPrefix =
            generatePathPrefix(prefix,
                              recordCollection.getKey().getCluster(),
                              recordCollection.getKey().getUserName(),
                              genAppId,
                              pigAliasFp)
                              .toString();
      } else {
        metricsPathPrefix =
            generatePathPrefix(prefix,
                              recordCollection.getKey().getCluster(),
                              recordCollection.getKey().getUserName(),
                              genAppId)
                              .toString();
      }
      
      try {
        storeAppIdMapping(metricsPathPrefix);
      } catch (IOException e) {
        LOG.error("Failed to store mapping for app " + recordCollection.getKey().getAppId()
            + " to '" + metricsPathPrefix + "'");
      }

      // Round the timestamp to second as Graphite accepts it in such
      // a format.
      int timestamp = Math.round(recordCollection.getSubmitTime() / 1000);
      
      // For now, relies on receiving job history and job conf as part of the same
      // JobHistoryMultiRecord
      for (JobHistoryRecord jobRecord : recordCollection) {
        if (service == HravenService.JOB_HISTORY
            && (jobRecord.getDataCategory() == RecordCategory.HISTORY_COUNTER || jobRecord
                .getDataCategory() == RecordCategory.INFERRED)
            && !(jobRecord.getDataKey().get(0).equalsIgnoreCase(submitTimeKey)
                || jobRecord.getDataKey().get(0).equalsIgnoreCase(launchTimeKey) || jobRecord.getDataKey()
                .get(0).equalsIgnoreCase(finishTimeKey))) {

          StringBuilder line = new StringBuilder();
          line.append(metricsPathPrefix);

          boolean ignoreRecord = false;
          for (String comp : jobRecord.getDataKey().getComponents()) {
            if (excludedComponents != null && excludedComponents.contains(comp)) {
              ignoreRecord = true;
              LOG.info("Excluding component '" + jobRecord.getDataKey().toString() + "' of app " + jobRecord.getKey().toString());
              break;
            }
            line.append(".").append(sanitize(comp));
          }
          
          if (ignoreRecord)
            continue;
          else
            lineCount++;

          line.append(" ").append(jobRecord.getDataValue()).append(" ")
              .append(timestamp).append("\n");
          lines.append(line);
        }
      }
      
      //handle run times
      Long finishTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(finishTimeKey));
      if (finishTime == null)
        finishTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(finishTimeKey.toLowerCase()));
      Long launchTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(launchTimeKey));
      if (launchTime == null)
        launchTime = (Long)recordCollection.getValue(RecordCategory.HISTORY_COUNTER, new RecordDataKey(launchTimeKey.toLowerCase()));
      
      if (finishTime != null && recordCollection.getSubmitTime() != null) {
        lines.append(metricsPathPrefix + ".").append(totalTimeKey + " " + (finishTime-recordCollection.getSubmitTime()) + " " + timestamp + "\n");
        lineCount++;
      }
      
      if (finishTime != null && launchTime != null) {
        lines.append(metricsPathPrefix + ".").append(runTimeKey + " " + (finishTime-launchTime) + " " + timestamp + "\n");
        lineCount++;
      }
    }
    
    return lineCount;
  }

  private void storeAppIdMapping(String metricsPathPrefix) throws IOException {
    Put put = new Put(new JobKeyConverter().toBytes(recordCollection.getKey()));
    put.add(Constants.INFO_FAM_BYTES, Constants.GRAPHITE_KEY_MAPPING_COLUMN_BYTES, Bytes.toBytes(metricsPathPrefix));
    keyMappingTable.put(put);
    
    put = new Put(Bytes.toBytes(metricsPathPrefix));
    
    byte[] appIdBytes = ByteUtil.join(Constants.SEP_BYTES,
      Bytes.toBytes(recordCollection.getKey().getCluster()),
      Bytes.toBytes(recordCollection.getKey().getUserName()),
      Bytes.toBytes(recordCollection.getKey().getAppId()));
      
    put.add(Constants.INFO_FAM_BYTES, Constants.GRAPHITE_KEY_MAPPING_COLUMN_BYTES, appIdBytes);
    reverseKeyMappingTable.put(put);
  }

  private String getPigAliasFingerprint(JobHistoryRecordCollection recordCollection) {
    Object aliasRec = recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("pig.alias"));
    Object featureRec = recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("pig.job.feature"));

    String alias = null;
    String feature = null;

    if (aliasRec != null) {
      alias = (String) aliasRec;
    }

    if (featureRec != null) {
      feature = (String) featureRec;
    }

    if (alias != null) {
      return (feature != null ? feature + ":" : "")
          + StringUtils.abbreviate(alias, PIG_ALIAS_FINGERPRINT_LENGTH);
    }

    return null;
  }

  private String genAppId(JobHistoryRecordCollection recordCollection, String appId) {
    String oozieActionName = getOozieActionName(recordCollection);

    if (getFramework(recordCollection) == Framework.PIG && APPID_PATTERN_PIGJOB.matcher(appId).matches()) {
      // pig:{oozie-action-name}:{pigscript}
      if (oozieActionName != null) {
        appId = APPID_PATTERN_PIGJOB.matcher(appId).replaceAll("pig:" + oozieActionName + ":$1");
      } else {
        appId = APPID_PATTERN_PIGJOB.matcher(appId).replaceAll("pig:$1");
      }
    } else if (APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).matches()) {
      // ozl:{oozie-workflow-name}
      appId = APPID_PATTERN_OOZIE_LAUNCHER.matcher(appId).replaceAll("ozl:$1:$2:$3");
    } else if (APPID_PATTERN_OOZIE_ACTION.matcher(appId).matches()) {
      // oza:{oozie-workflow-name}:{oozie-action-name}
      appId = APPID_PATTERN_OOZIE_ACTION.matcher(appId).replaceAll("oza:$1:$2:$3:$4");
    }

    return appId;
  }

  private Framework getFramework(JobHistoryRecordCollection recordCollection) {
    Object rec =
        recordCollection.getValue(RecordCategory.CONF_META, new RecordDataKey(Constants.FRAMEWORK_COLUMN));

    if (rec != null) {
      return Framework.valueOf((String) rec);
    }

    return null;
  }

  private String getOozieActionName(JobHistoryRecordCollection recordCollection) {
    Object rec = recordCollection.getValue(RecordCategory.CONF, new RecordDataKey("oozie.action.id"));

    if (rec != null) {
      String actionId = ((String) rec);
      return actionId.substring(actionId.indexOf("@") + 1, actionId.length());
    }

    return null;
  }

  /**
   * Util method to generate metrix path prefix
   * @return
   * @throws UnsupportedEncodingException
   */

  private StringBuilder generatePathPrefix(String... args) {
    StringBuilder prefix = new StringBuilder();
    boolean first = true;
    for (String arg : args) {
      if (!first) {
        prefix.append(".");
      }

      prefix.append(sanitize(arg));
      first = false;
    }
    return prefix;
  }

  /**
   * Util method to sanitize metrics for sending to graphite E.g. remove periods ("."), etc.
   * @throws UnsupportedEncodingException
   */
  public static String sanitize(String s) {
    return s.replaceAll(GRAPHITE_KEY_FILTER, "_");
  }

  /**
   * Output the {@link JobHistoryRecord} received in debug log
   * @param serviceKey
   * @param jobRecord
   */

  public static void logRecord(HravenService serviceKey, JobHistoryRecord jobRecord) {
    StringBuilder line = new StringBuilder();
    String seperator = ", ";
    String seperator2 = "|";

    line.append("Service: " + serviceKey.name());

    JobKey key = jobRecord.getKey();
    line.append(seperator).append("Cluster: " + key.getCluster());
    line.append(seperator).append("User: " + key.getUserName());
    line.append(seperator).append("AppId: " + key.getAppId());
    line.append(seperator).append("RunId: " + key.getRunId());
    line.append(seperator).append("JobId: " + key.getJobId());

    line.append(seperator2);
    line.append(seperator).append("Category: " + jobRecord.getDataCategory().name());

    line.append(seperator).append("Key: ");
    for (String comp : jobRecord.getDataKey().getComponents()) {
      line.append(comp).append(".");
    }

    line.append(seperator).append("Value: " + jobRecord.getDataValue());
    line.append(seperator).append("SubmitTime: " + jobRecord.getSubmitTime());

    LOG.debug(line);
  }
}
