/*
 * Copyright 2013 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven.etl;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.configuration.ConversionException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Maps;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.util.ByteArrayWrapper;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.TaskKeyConverter;

/**
 * Deal with JobHistory file parsing for job history files which are generated after MAPREDUCE-1016
 * (hadoop 1.x (0.21 and later) and hadoop 2.x)
 */
public class JobHistoryFileParserHadoop2 extends JobHistoryFileParserBase {

  private JobKey jobKey;
  /** Job ID, minus the leading "job_" */
  private String jobNumber = "";
  private byte[] jobKeyBytes;
  private List<Put> jobPuts = new LinkedList<Put>();
  private List<Put> taskPuts = new LinkedList<Put>();
  boolean uberized = false;

  /**
   * Stores the terminal status of the job
   *
   * Since this history file is placed hdfs at mapreduce.jobhistory.done-dir
   * only upon job termination, we ensure that we store the status seen
   * only in one of the terminal state events in the file like
   * JobFinished(JOB_FINISHED) or JobUnsuccessfulCompletion(JOB_FAILED, JOB_KILLED)
   *
   * Ideally, each terminal state event like JOB_FINISHED, JOB_FAILED, JOB_KILLED
   * should contain the jobStatus field and we would'nt need this extra processing
   * But presently, in history files, only JOB_FAILED, JOB_KILLED events
   * contain the jobStatus field where as JOB_FINISHED event does not,
   * hence this extra processing
   */
  private String jobStatus = "";
  /** hadoop2 JobState enum:
   * NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR
   */
  public static final String JOB_STATUS_SUCCEEDED = "SUCCEEDED";

  /** explicitly initializing map millis and
   * reduce millis in case it's not found
   */
  private long mapSlotMillis = 0L;
  private long reduceSlotMillis = 0L;

  private long startTime = Constants.NOTFOUND_VALUE;
  private long endTime = Constants.NOTFOUND_VALUE;
  private static final String LAUNCH_TIME_KEY_STR = JobHistoryKeys.LAUNCH_TIME.toString();
  private static final String FINISH_TIME_KEY_STR = JobHistoryKeys.FINISH_TIME.toString();

  private JobKeyConverter jobKeyConv = new JobKeyConverter();
  private TaskKeyConverter taskKeyConv = new TaskKeyConverter();

  private static final String AM_ATTEMPT_PREFIX = "AM_";
  private static final String TASK_PREFIX = "task_";
  private static final String TASK_ATTEMPT_PREFIX = "attempt_";

  private static final Log LOG = LogFactory.getLog(JobHistoryFileParserHadoop2.class);

  private Schema schema;
  private Decoder decoder;
  private DatumReader<GenericRecord> reader;

  private static final String TYPE = "type";
  private static final String EVENT = "event";
  private static final String NAME = "name";
  private static final String FIELDS = "fields";
  private static final String COUNTS = "counts";
  private static final String GROUPS = "groups";
  private static final String VALUE = "value";
  private static final String TASKID = "taskid";
  private static final String APPLICATION_ATTEMPTID = "applicationAttemptId";
  private static final String ATTEMPTID = "attemptId";

  private static final String TYPE_INT = "int";
  private static final String TYPE_BOOLEAN = "boolean";
  private static final String TYPE_LONG = "long";
  private static final String TYPE_STRING = "String";
  /** only acls in the job history file seem to be of this type: map of strings */
  private static final String TYPE_MAP_STRINGS = "{\"type\":\"map\",\"values\":\"string\"}";
  /**
   * vMemKbytes, clockSplit, physMemKbytes, cpuUsages are arrays of ints See MAPREDUCE-5432
   */
  private static final String TYPE_ARRAY_INTS = "{\"type\":\"array\",\"items\":\"int\"}";
  /** this is part of {@link org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent.java} */
  private static final String NULL_STRING = "[\"null\",\"string\"]";

  public static enum Hadoop2RecordType {
    /**
     * populating this map since the symbols and key to get the types of fields the symbol denotes
     * the record in the file (like JOB_SUBMITTED) and it's value in the map (like JobSubmitted)
     * helps us get the types of fields that that record contains (this type information is present
     * in the schema)
     */
    JobFinished("JOB_FINISHED"),
    JobInfoChange("JOB_INFO_CHANGED"),
    JobInited("JOB_INITED"),
    AMStarted("AM_STARTED"),
    JobPriorityChange("JOB_PRIORITY_CHANGED"),
    JobStatusChanged("JOB_STATUS_CHANGED"),
    JobQueueChange("JOB_QUEUE_CHANGED"),
    JobSubmitted("JOB_SUBMITTED"),
    JobUnsuccessfulCompletion("JOB_KILLED","JOB_FAILED"),
    MapAttemptFinished("MAP_ATTEMPT_FINISHED"),
    ReduceAttemptFinished("REDUCE_ATTEMPT_FINISHED"),
    TaskAttemptFinished("CLEANUP_ATTEMPT_FINISHED"),
    TaskAttemptStarted("CLEANUP_ATTEMPT_STARTED",
      "SETUP_ATTEMPT_STARTED",
      "REDUCE_ATTEMPT_STARTED",
      "MAP_ATTEMPT_STARTED"),
    TaskAttemptUnsuccessfulCompletion("CLEANUP_ATTEMPT_KILLED",
      "CLEANUP_ATTEMPT_FAILED",
      "SETUP_ATTEMPT_KILLED",
      "SETUP_ATTEMPT_FAILED",
      "REDUCE_ATTEMPT_KILLED",
      "REDUCE_ATTEMPT_FAILED",
      "MAP_ATTEMPT_KILLED",
      "MAP_ATTEMPT_FAILED"),
    TaskFailed("TASK_FAILED"),
    TaskFinished("TASK_FINISHED"),
    TaskStarted("TASK_STARTED"),
    TaskUpdated("TASK_UPDATED");

    private final String[] recordNames;

    private Hadoop2RecordType(String... recordNames) {
      if (recordNames != null) {
        this.recordNames = recordNames;
      } else {
        this.recordNames = new String[0];
      }
    }

    public String[] getRecordNames() {
      return recordNames;
    }
  }

  public static enum CounterTypes {
    counters, mapCounters, reduceCounters, totalCounters
  }
  private static Map<String,Hadoop2RecordType> EVENT_RECORD_NAMES = Maps.newHashMap();
  private static final Set<String> COUNTER_NAMES = new HashSet<String>();
  private Map<Hadoop2RecordType, Map<String, String>> fieldTypes =
      new HashMap<Hadoop2RecordType, Map<String, String>>();

  /**
   * populates the COUNTER_NAMES hash set and EVENT_RECORD_NAMES hash map
   */
  static {
    /**
     * populate the hash set for counter names
     */
    for (CounterTypes ct : CounterTypes.values()) {
      COUNTER_NAMES.add(ct.toString());
    }

    /**
     * populate the hash map of EVENT_RECORD_NAMES
     */
    for (Hadoop2RecordType t : Hadoop2RecordType.values()) {
      for (String name : t.getRecordNames()) {
        EVENT_RECORD_NAMES.put(name, t);
      }
    }

  }

  JobHistoryFileParserHadoop2(Configuration conf) {
    super(conf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void parse(byte[] historyFileContents, JobKey jobKey)
      throws ProcessingException {

    this.jobKey = jobKey;
    this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
    setJobId(jobKey.getJobId().getJobIdString());

    try {
      FSDataInputStream in =
          new FSDataInputStream(new ByteArrayWrapper(historyFileContents));

      /** first line is the version, ignore it */
      String versionIgnore = in.readLine();

      /** second line in file is the schema */
      this.schema = schema.parse(in.readLine());

      /** now figure out the schema */
      understandSchema(schema.toString());

      /** now read the rest of the file */
      this.reader = new GenericDatumReader<GenericRecord>(schema);
      this.decoder = DecoderFactory.get().jsonDecoder(schema, in);

      GenericRecord record = null;
      Hadoop2RecordType recType = null;
      try {
        while ((record = reader.read(null, decoder)) != null) {
          if (record.get(TYPE) != null) {
            recType = EVENT_RECORD_NAMES.get(record.get(TYPE).toString());
          } else {
            throw new ProcessingException("expected one of "
                + Arrays.asList(Hadoop2RecordType.values())
                + " \n but not found, cannot process this record! " + jobKey);
          }
          if (recType == null) {
            throw new ProcessingException("new record type has surfaced: "
                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
          }
          // GenericRecord's get returns an Object
          Object eDetails = record.get(EVENT);

          // confirm that we got an "event" object
          if (eDetails != null) {
            JSONObject eventDetails = new JSONObject(eDetails.toString());
            processRecords(recType, eventDetails);
          } else {
            throw new ProcessingException("expected event details but not found "
                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
          }
        }
      } catch (EOFException eof) {
        // not an error, simply end of file
        LOG.info("Done parsing file, reached eof for " + jobKey);
      }
    } catch (IOException ioe) {
      throw new ProcessingException(" Unable to parse history file in function parse, "
          + "cannot process this record!" + jobKey + " error: ", ioe);
    } catch (JSONException jse) {
      throw new ProcessingException(" Unable to parse history file in function parse, "
          + "cannot process this record! " + jobKey + " error: ", jse);
    } catch (IllegalArgumentException iae) {
      throw new ProcessingException(" Unable to parse history file in function parse, "
          + "cannot process this record! " + jobKey + " error: ", iae);
    }

    /*
     * set the job status for this job once the entire file is parsed
     * this has to be done separately
     * since JOB_FINISHED event is missing the field jobStatus,
     * where as JOB_KILLED and JOB_FAILED
     * events are not so we need to look through the whole file to confirm
     * the job status and then generate the put
     */
    Put jobStatusPut = getJobStatusPut();
    this.jobPuts.add(jobStatusPut);

    // set the hadoop version for this record
    Put versionPut = getHadoopVersionPut(JobHistoryFileParserFactory.getHistoryFileVersion2(), this.jobKeyBytes);
    this.jobPuts.add(versionPut);

    LOG.info("For " + this.jobKey + " #jobPuts " + jobPuts.size() + " #taskPuts: "
        + taskPuts.size());
  }

  /**
   * generates a put for job status
   * @return Put that contains Job Status
   */
  private Put getJobStatusPut() {
    Put pStatus = new Put(jobKeyBytes);
    byte[] valueBytes = Bytes.toBytes(this.jobStatus);
    byte[] qualifier = Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase());
    pStatus.add(Constants.INFO_FAM_BYTES, qualifier, valueBytes);
    return pStatus;
  }

  /**
   * understand the schema so that we can parse the rest of the file
   * @throws JSONException
   */
  private void understandSchema(String schema) throws JSONException {

    JSONObject j1 = new JSONObject(schema);
    JSONArray fields = j1.getJSONArray(FIELDS);

    String fieldName;
    String fieldTypeValue;
    Object recName;

    for (int k = 0; k < fields.length(); k++) {
      if (fields.get(k) == null) {
        continue;
      }
      JSONObject allEvents = new JSONObject(fields.get(k).toString());
      Object name = allEvents.get(NAME);
      if (name != null) {
        if (name.toString().equalsIgnoreCase(EVENT)) {
          JSONArray allTypeDetails = allEvents.getJSONArray(TYPE);
          for (int i = 0; i < allTypeDetails.length(); i++) {
            JSONObject actual = (JSONObject) allTypeDetails.get(i);
            JSONArray types = actual.getJSONArray(FIELDS);
            Map<String, String> typeDetails = new HashMap<String, String>();
            for (int j = 0; j < types.length(); j++) {
              if (types.getJSONObject(j) == null ) {
                continue;
              }
              fieldName = types.getJSONObject(j).getString(NAME);
              fieldTypeValue = types.getJSONObject(j).getString(TYPE);
              if ((fieldName != null) && (fieldTypeValue != null)) {
                typeDetails.put(fieldName, fieldTypeValue);
              }
            }

            recName = actual.get(NAME);
            if (recName != null) {
              /* the next statement may throw an IllegalArgumentException if
               * it finds a new string that's not part of the Hadoop2RecordType enum
               * that way we know what types of events we are parsing
               */
              fieldTypes.put(Hadoop2RecordType.valueOf(recName.toString()), typeDetails);
            }
          }
        }
      }
    }
  }

  /**
   * process the counter details example line in .jhist file for counters: { "name":"MAP_COUNTERS",
   * "groups":[ { "name":"org.apache.hadoop.mapreduce.FileSystemCounter",
   * "displayName":"File System Counters", "counts":[ { "name":"HDFS_BYTES_READ",
   * "displayName":"HDFS: Number of bytes read", "value":480 }, { "name":"HDFS_BYTES_WRITTEN",
   * "displayName":"HDFS: Number of bytes written", "value":0 } ] }, {
   * "name":"org.apache.hadoop.mapreduce.TaskCounter", "displayName":"Map-Reduce Framework",
   * "counts":[ { "name":"MAP_INPUT_RECORDS", "displayName":"Map input records", "value":10 }, {
   * "name":"MAP_OUTPUT_RECORDS", "displayName":"Map output records", "value":10 } ] } ] }
   */
  private void processCounters(Put p, JSONObject eventDetails, String key) {

    try {
      JSONObject jsonCounters = eventDetails.getJSONObject(key);
      String counterMetaGroupName = jsonCounters.getString(NAME);
      JSONArray groups = jsonCounters.getJSONArray(GROUPS);
      for (int i = 0; i < groups.length(); i++) {
        JSONObject aCounter = groups.getJSONObject(i);
        JSONArray counts = aCounter.getJSONArray(COUNTS);
        for (int j = 0; j < counts.length(); j++) {
          JSONObject countDetails = counts.getJSONObject(j);
          populatePut(p, Constants.INFO_FAM_BYTES, counterMetaGroupName, aCounter.get(NAME)
              .toString(), countDetails.get(NAME).toString(), countDetails.getLong(VALUE));
        }
      }
    } catch (JSONException e) {
      throw new ProcessingException(" Caught json exception while processing counters ", e);
    }

  }

  /**
   * process the event details as per their data type from schema definition
   * @throws JSONException
   */
  private void
      processAllTypes(Put p, Hadoop2RecordType recType, JSONObject eventDetails, String key)
          throws JSONException {

    if (COUNTER_NAMES.contains(key)) {
      processCounters(p, eventDetails, key);
    } else {
      String type = fieldTypes.get(recType).get(key);
      if (type.equalsIgnoreCase(TYPE_STRING)) {
        // look for job status
        if (JobHistoryKeys.JOB_STATUS.toString().equals(
          JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
          // store it only if it's one of the terminal state events
          if ((recType.equals(Hadoop2RecordType.JobFinished))
              || (recType.equals(Hadoop2RecordType.JobUnsuccessfulCompletion))) {
            this.jobStatus = eventDetails.getString(key);
          }
        } else {
          String value = eventDetails.getString(key);
          populatePut(p, Constants.INFO_FAM_BYTES, key, value);
        }
      } else if (type.equalsIgnoreCase(TYPE_LONG)) {
        long value = eventDetails.getLong(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
        // populate start time of the job for megabytemillis calculations
        if ((recType.equals(Hadoop2RecordType.JobInited)) &&
            LAUNCH_TIME_KEY_STR.equals(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
          this.startTime = value;
        }
        // populate end time of the job for megabytemillis calculations
        if ((recType.equals(Hadoop2RecordType.JobFinished))
            || (recType.equals(Hadoop2RecordType.JobUnsuccessfulCompletion))) {
          if (FINISH_TIME_KEY_STR.equals(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key))) {
            this.endTime = value;
          }
        }
      } else if (type.equalsIgnoreCase(TYPE_INT)) {
        int value = eventDetails.getInt(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_BOOLEAN)) {
        boolean value = eventDetails.getBoolean(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, Boolean.toString(value));
      } else if (type.equalsIgnoreCase(TYPE_ARRAY_INTS)) {
        String value = eventDetails.getString(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(NULL_STRING)) {
        // usually seen in FAILED tasks
        String value = eventDetails.getString(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_MAP_STRINGS)) {
        JSONObject ms = new JSONObject(eventDetails.get(key).toString());
        populatePut(p, Constants.INFO_FAM_BYTES, key, ms.toString());
      } else {
        throw new ProcessingException("Encountered a new type " + type
            + " unable to complete processing " + this.jobKey);
      }
    }
  }

  /**
   * iterate over the event details and prepare puts
   * @throws JSONException
   */
  private void iterateAndPreparePuts(JSONObject eventDetails, Put p, Hadoop2RecordType recType)
      throws JSONException {
    Iterator<?> keys = eventDetails.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      processAllTypes(p, recType, eventDetails, key);
    }
  }

  /**
   * process individual records
   * @throws JSONException
   */
  private void processRecords(Hadoop2RecordType recType, JSONObject eventDetails)
      throws JSONException {

    switch (recType) {
    case JobFinished:
      // this setting is needed since the job history file is missing
      // the jobStatus field in the JOB_FINISHED event
      this.jobStatus = JOB_STATUS_SUCCEEDED;
    case JobInfoChange:
    case JobInited:
    case JobPriorityChange:
    case JobStatusChanged:
    case JobQueueChange:
    case JobSubmitted:
    case JobUnsuccessfulCompletion:
      Put pJob = new Put(this.jobKeyBytes);
      iterateAndPreparePuts(eventDetails, pJob, recType);
      this.jobPuts.add(pJob);
      break;

    case AMStarted:
      byte[] amAttemptIdKeyBytes =
          getAMKey(AM_ATTEMPT_PREFIX, eventDetails.getString(APPLICATION_ATTEMPTID));
      // generate a new put per AM Attempt
      Put pAM = new Put(amAttemptIdKeyBytes);
      pAM.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pAM, recType);
      taskPuts.add(pAM);
      break;

    case MapAttemptFinished:
      byte[] taskMAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pMTaskAttempt = new Put(taskMAttemptIdKeyBytes);
      pMTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.MapAttempt.toString()));
      iterateAndPreparePuts(eventDetails, pMTaskAttempt, recType);
      this.taskPuts.add(pMTaskAttempt);
      break;

    case ReduceAttemptFinished:
      byte[] taskRAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pRTaskAttempt = new Put(taskRAttemptIdKeyBytes);
      pRTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.ReduceAttempt.toString()));
      iterateAndPreparePuts(eventDetails, pRTaskAttempt, recType);
      this.taskPuts.add(pRTaskAttempt);
      break;

    case TaskAttemptFinished:
    case TaskAttemptStarted:
    case TaskAttemptUnsuccessfulCompletion:
      byte[] taskAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pTaskAttempt = new Put(taskAttemptIdKeyBytes);
      pTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pTaskAttempt, recType);
      taskPuts.add(pTaskAttempt);
      break;

    case TaskFailed:
    case TaskStarted:
    case TaskUpdated:
    case TaskFinished:
      byte[] taskIdKeyBytes =
          getTaskKey(TASK_PREFIX, this.jobNumber, eventDetails.getString(TASKID));
      Put pTask = new Put(taskIdKeyBytes);
      pTask.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pTask, recType);
      taskPuts.add(pTask);
      break;
    default:
      LOG.error("Check if recType was modified and has new members?");
      throw new ProcessingException("Check if recType was modified and has new members? " + recType);
    }
  }

  /**
   * Sets the job ID and strips out the job number (job ID minus the "job_" prefix).
   * @param id
   */
  private void setJobId(String id) {
    if (id != null && id.startsWith("job_") && id.length() > 4) {
      this.jobNumber = id.substring(4);
    }
  }

  /**
   * maintains compatibility between hadoop 1.0 keys and hadoop 2.0 keys. It also confirms that this
   * key exists in JobHistoryKeys enum
   * @throws IllegalArgumentException NullPointerException
   */
  private String getKey(String key) throws IllegalArgumentException {
    String checkKey =
        JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.containsKey(key) ? JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING
            .get(key) : key;
    return (JobHistoryKeys.valueOf(checkKey).toString());
  }

  /**
   * populates a put for long values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param long value
   */
  private void populatePut(Put p, byte[] family, String key, long value) {

    byte[] valueBytes = null;
    valueBytes = (value != 0L) ? Bytes.toBytes(value) : Constants.ZERO_LONG_BYTES;
    byte[] qualifier = Bytes.toBytes(getKey(key).toLowerCase());
    p.add(family, qualifier, valueBytes);
  }

  /**
   * gets the int values as ints or longs some keys in 2.0 are now int, they were longs in 1.0 this
   * will maintain compatiblity between 1.0 and 2.0 by casting those ints to long
   *
   * keeping this function package level visible (unit testing)
   * @throws IllegalArgumentException if new key is encountered
   */
   byte[] getValue(String key, int value) {
    byte[] valueBytes = null;
    Class<?> clazz = JobHistoryKeys.KEY_TYPES.get(JobHistoryKeys.valueOf(key));
    if (clazz == null) {
      throw new IllegalArgumentException(" unknown key " + key + " encountered while parsing "
          + this.jobKey);
    }
    if (Long.class.equals(clazz)) {
      valueBytes = (value != 0L) ? Bytes.toBytes(new Long(value)) : Constants.ZERO_LONG_BYTES;
    } else {
      valueBytes = (value != 0) ? Bytes.toBytes(value) : Constants.ZERO_INT_BYTES;
    }
    return valueBytes;
  }

  /**
   * populates a put for int values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param int value
   */
  private void populatePut(Put p, byte[] family, String key, int value) {

    String jobHistoryKey = getKey(key);
    byte[] valueBytes = getValue(jobHistoryKey, value);
    byte[] qualifier = Bytes.toBytes(jobHistoryKey.toLowerCase());
    p.add(family, qualifier, valueBytes);
  }

  /**
   * populates a put for string values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param {@link String} key
   * @param String value
   */
  private void populatePut(Put p, byte[] family, String key, String value) {
    byte[] valueBytes = null;
    valueBytes = Bytes.toBytes(value);
    byte[] qualifier = Bytes.toBytes(getKey(key).toLowerCase());
    p.add(family, qualifier, valueBytes);
  }

  /**
   * populates a put for {@link Counters}
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param String groupName
   * @param String counterName
   * @param long counterValue
   */
  private void populatePut(Put p, byte[] family, String key, String groupName, String counterName,
      Long counterValue) {
    byte[] counterPrefix = null;

    try {
      switch (JobHistoryKeys.valueOf(JobHistoryKeys.class, key)) {
      case COUNTERS:
      case TOTAL_COUNTERS:
      case TASK_COUNTERS:
      case TASK_ATTEMPT_COUNTERS:
        counterPrefix = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case MAP_COUNTERS:
        counterPrefix = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case REDUCE_COUNTERS:
        counterPrefix =
            Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      default:
        throw new IllegalArgumentException("Unknown counter type " + key.toString());
      }
    } catch (IllegalArgumentException iae) {
      throw new ProcessingException("Unknown counter type " + key, iae);
    } catch (NullPointerException npe) {
      throw new ProcessingException("Null counter type " + key, npe);
    }

    byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(groupName), Constants.SEP_BYTES);
    byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counterName));

    /**
     * correct and populate map and reduce slot millis
     */
    if ((Constants.SLOTS_MILLIS_MAPS.equals(counterName)) ||
        (Constants.SLOTS_MILLIS_REDUCES.equals(counterName))) {
      counterValue = getStandardizedCounterValue(counterName, counterValue);
    }

    p.add(family, qualifier, Bytes.toBytes(counterValue));

  }

  private long getMemoryMb(String key) {
    long memoryMb = 0L;
    if (Constants.MAP_MEMORY_MB_CONF_KEY.equals(key)){
	memoryMb =  this.jobConf.getLong(key, Constants.DEFAULT_MAP_MEMORY_MB);
    }else if (Constants.REDUCE_MEMORY_MB_CONF_KEY.equals(key)){
	memoryMb = this.jobConf.getLong(key, Constants.DEFAULT_REDUCE_MEMORY_MB);
    }
    if (memoryMb == 0L) {
      throw new ProcessingException(
          "While correcting slot millis, " + key + " was found to be 0 ");
    }
    return memoryMb;
  }

  /**
   * Issue #51 in hraven on github
   * map and reduce slot millis in Hadoop 2.0 are not calculated properly.
   * They are aproximately 4X off by actual value.
   * calculate the correct map slot millis as
   * hadoop2ReportedMapSlotMillis * yarn.scheduler.minimum-allocation-mb
   *        / mapreduce.mapreduce.memory.mb
   * similarly for reduce slot millis
   * @param counterName
   * @param counterValue
   * @return corrected counter value
   */
  private Long getStandardizedCounterValue(String counterName, Long counterValue) {
    if (jobConf == null) {
      throw new ProcessingException("While correcting slot millis, jobConf is null");
    }
    long yarnSchedulerMinMB = this.jobConf.getLong(Constants.YARN_SCHEDULER_MIN_MB,
          Constants.DEFAULT_YARN_SCHEDULER_MIN_MB);
    long updatedCounterValue = 0L;
    long memoryMb = 0L;
    String key;
    if (Constants.SLOTS_MILLIS_MAPS.equals(counterName)) {
      key = Constants.MAP_MEMORY_MB_CONF_KEY;
      memoryMb = getMemoryMb(key);
      updatedCounterValue = counterValue * yarnSchedulerMinMB / memoryMb;
      this.mapSlotMillis = updatedCounterValue;
    } else {
      key = Constants.REDUCE_MEMORY_MB_CONF_KEY;
      memoryMb = getMemoryMb(key);
      updatedCounterValue = counterValue * yarnSchedulerMinMB / memoryMb;
      this.reduceSlotMillis = updatedCounterValue;
    }

    LOG.info("Updated " + counterName + " from " + counterValue + " to " + updatedCounterValue
        + " based on " + Constants.YARN_SCHEDULER_MIN_MB + ": " + yarnSchedulerMinMB
        + " and " + key + ": " + memoryMb);
    return updatedCounterValue;
  }

  /**
   * Returns the Task ID or Task Attempt ID, stripped of the leading job ID, appended to the job row
   * key.
   */
  public byte[] getTaskKey(String prefix, String jobNumber, String fullId) {
    String taskComponent = fullId;
    if (fullId == null) {
      taskComponent = "";
    } else {
      String expectedPrefix = prefix + jobNumber + "_";
      if ((fullId.startsWith(expectedPrefix)) && (fullId.length() > expectedPrefix.length())) {
        taskComponent = fullId.substring(expectedPrefix.length());
      }
    }
    return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
  }

  /**
   * Returns the AM Attempt id stripped of the leading job ID, appended to the job row key.
   */
  public byte[] getAMKey(String prefix, String fullId) {

    String taskComponent = prefix + fullId;
    return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Put> getJobPuts() {
    return jobPuts;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Put> getTaskPuts() {
    return taskPuts;
  }

  /**
   * utitlity function for printing all puts
   */
  public void printAllPuts(List<Put> p) {
    for (Put p1 : p) {
      Map<byte[], List<KeyValue>> d = p1.getFamilyMap();
      for (byte[] k : d.keySet()) {
        System.out.println(" k " + Bytes.toString(k));
      }
      for (List<KeyValue> lkv : d.values()) {
        for (KeyValue kv : lkv) {
          System.out.println("\n row: " + taskKeyConv.fromBytes(kv.getRow())
              + "\n " + Bytes.toString(kv.getQualifier()) + ": " + Bytes.toString(kv.getValue()));
        }
      }
    }
  }

  /**
   * calculate mega byte millis puts as: 
   * if not uberized: 
   *        map slot millis * mapreduce.map.memory.mb
   *        + reduce slot millis * mapreduce.reduce.memory.mb 
   *        + yarn.app.mapreduce.am.resource.mb * job runtime 
   * if uberized:
   *        yarn.app.mapreduce.am.resource.mb * job run time
   */
  @Override
  public Long getMegaByteMillis() {

    if (endTime == Constants.NOTFOUND_VALUE || startTime == Constants.NOTFOUND_VALUE)
    {
      throw new ProcessingException("Cannot calculate megabytemillis for " + jobKey
          + " since one or more of endTime " + endTime + " startTime " + startTime
          + " not found!");
    }

    long jobRunTime = 0L;
    long amMb = 0L;
    long mapMb = 0L;
    long reduceMb = 0L;

    jobRunTime = endTime - startTime;

    if (jobConf == null) {
      LOG.error("job conf is null? for job key: " + jobKey.toString());
      return null;
    }

    // get am memory mb, map memory mb, reducer memory mb from job conf
    try {
      amMb = jobConf.getLong(Constants.AM_MEMORY_MB_CONF_KEY, Constants.NOTFOUND_VALUE);
      mapMb = jobConf.getLong(Constants.MAP_MEMORY_MB_CONF_KEY,  Constants.NOTFOUND_VALUE);
      reduceMb = jobConf.getLong(Constants.REDUCE_MEMORY_MB_CONF_KEY,  Constants.NOTFOUND_VALUE);
    } catch (ConversionException ce) {
      LOG.error(" Could not convert to long " + ce.getMessage());
      throw new ProcessingException(
          " Can't calculate megabytemillis since conversion to long failed", ce);
    }
    if (amMb == Constants.NOTFOUND_VALUE ) {
      throw new ProcessingException("Cannot calculate megabytemillis for " + jobKey
          + " since " + Constants.AM_MEMORY_MB_CONF_KEY + " not found!");
    }

    Long mbMillis = 0L;
    if (uberized) {
      mbMillis = amMb * jobRunTime;
    } else {
      mbMillis = (mapMb * mapSlotMillis) + (reduceMb * reduceSlotMillis) + (amMb * jobRunTime);
    }

    LOG.debug("For " + jobKey.toString() + " " + Constants.MEGABYTEMILLIS + " is " + mbMillis
        + " since \n uberized: " + uberized + " \n " + "mapMb: " + mapMb + " mapSlotMillis: "
        + mapSlotMillis + " \n " + " reduceMb: " + reduceMb + " reduceSlotMillis: "
        + reduceSlotMillis + " \n " + " amMb: " + amMb + " jobRunTime: " + jobRunTime
        + " start time: " + this.startTime + " endtime " + this.endTime);

    return mbMillis;
  }
}
