package com.twitter.hraven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author angad.singh Abstraction of a record to be stored in the {@link HravenService#JOB_HISTORY}
 *         service. Was earlier directly written as an Hbase put
 */

public class JobHistoryRecord extends HravenRecord<JobKey, Object> {

  public JobHistoryRecord(RecordCategory dataCategory, JobKey key, RecordDataKey dataKey,
      Object dataValue) {
    this.setKey(key);
    this.setDataCategory(dataCategory);
    this.setDataKey(dataKey);
    this.setDataValue(dataValue);
  }

  public JobHistoryRecord(RecordCategory dataCategory, JobKey key, RecordDataKey dataKey,
      Object dataValue, Long submitTime) {
    this(dataCategory, key, dataKey, dataValue);
    setSubmitTime(submitTime);
  }

  public JobHistoryRecord() {

  }

  public JobHistoryRecord(JobKey jobKey) {
    this.setKey(jobKey);
  }

  public void set(RecordCategory category, RecordDataKey key, String value) {
    this.setDataCategory(category);
    this.setDataKey(key);
    this.setDataValue(value);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }
}
