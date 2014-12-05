/*
Copyright 2014 Twitter, Inc.

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.twitter.hraven.Constants;
import com.twitter.hraven.HistoryFileType;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.util.ByteUtil;

public class JobHistoryFileParserSpark extends JobHistoryFileParserBase {

  private List<Put> jobPuts = new LinkedList<Put>();
  private List<Put> taskPuts = new LinkedList<Put>();
  private JobKeyConverter jobKeyConv = new JobKeyConverter();
  private long megabytemillis;
  private static final Log LOG = LogFactory.getLog(JobHistoryFileParserSpark.class);

  public JobHistoryFileParserSpark(Configuration jobConf) {
    super(jobConf);
  }

  @Override
  public void parse(byte[] historyFile, JobKey jobKey) {
    byte[] jobKeyBytes = jobKeyConv.toBytes(jobKey);
    Put sparkJobPuts = new Put(jobKeyBytes);

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      JsonNode rootNode = objectMapper.readTree(new ByteArrayInputStream(historyFile));
      String key;
      byte[] qualifier;
      byte[] valueBytes;

     Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.getFields();
     while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> field = fieldsIterator.next();
        // job history keys are in upper case
        key = field.getKey().toUpperCase();
        try {
          if (JobHistoryKeys.valueOf(key) != null) {
            qualifier = Bytes.toBytes(key.toString().toLowerCase());
            Class<?> clazz = JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.valueOf(key));
            if (clazz == null) {
              throw new IllegalArgumentException(" unknown key " + key
                  + " encountered while parsing " + key);
            }
            if (Integer.class.equals(clazz)) {
              try {
                valueBytes = Bytes.toBytes(field.getValue().getIntValue());
              } catch (NumberFormatException nfe) {
                // us a default value
                valueBytes = Constants.ZERO_INT_BYTES;
              }
            } else if (Long.class.equals(clazz)) {
              try {
                valueBytes = Bytes.toBytes(field.getValue().getLongValue());
              } catch (NumberFormatException nfe) {
                // us a default value
                valueBytes = Constants.ZERO_LONG_BYTES;
              }
            } else {
              // keep the string representation by default
              valueBytes = Bytes.toBytes(field.getValue().getTextValue());
            }
          } else {
            // simply store the key value as is
            qualifier = Bytes.toBytes(key.toLowerCase());
            valueBytes = Bytes.toBytes(field.getValue().getTextValue());
          }
        } catch (IllegalArgumentException iae) {
          // job history key does not exist, so
          // it could be 'queue' or 'megabytemillis' or 'batch.desc' field
          // store them as per hadoop1/hadoop2 compatible storage
          // if none fo those,
          // store the key value as is
          if (StringUtils.equals(key.toLowerCase(), Constants.HRAVEN_QUEUE)) {
            // queue is stored as part of job conf
            qualifier =
                ByteUtil.join(Constants.SEP_BYTES, Constants.JOB_CONF_COLUMN_PREFIX_BYTES,
                  Constants.HRAVEN_QUEUE_BYTES);
            valueBytes = Bytes.toBytes(field.getValue().getTextValue());

          } else if (StringUtils.equals(key.toLowerCase(), Constants.MEGABYTEMILLIS)) {
            qualifier = Constants.MEGABYTEMILLIS_BYTES;
            this.megabytemillis = field.getValue().getLongValue();
            valueBytes = Bytes.toBytes(this.megabytemillis);
          } else {
            // simply store the key as is
            qualifier = Bytes.toBytes(key.toLowerCase());
            valueBytes = Bytes.toBytes(field.getValue().getTextValue());
          }
        }
        sparkJobPuts.add(Constants.INFO_FAM_BYTES, qualifier, valueBytes);
      }
    } catch (JsonProcessingException e) {
      throw new ProcessingException("Caught exception during spark history parsing ", e);
    } catch (IOException e) {
      throw new ProcessingException("Caught exception during spark history parsing ", e);
    }
    this.jobPuts.add(sparkJobPuts);
    // set the history file type for this record
    Put historyFileTypePut = getHistoryFileTypePut(HistoryFileType.SPARK, jobKeyBytes);
    this.jobPuts.add(historyFileTypePut);

    LOG.info("For " + this.jobKeyConv.fromBytes(jobKeyBytes) + " #jobPuts " + jobPuts.size()
        + " #taskPuts: " + taskPuts.size());

  }

  @Override
  public Long getMegaByteMillis() {
    return this.megabytemillis;
  }

  @Override
  public List<Put> getJobPuts() {
    return this.jobPuts;
  }

  @Override
  public List<Put> getTaskPuts() {
    return this.taskPuts;
  }

}
