package com.twitter.hraven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class JobHistoryRawRecord extends HravenRecord<Text, Object> {

  public JobHistoryRawRecord(RecordCategory dataCategory, String key, RecordDataKey dataKey,
      Object dataValue) {
    this.setKey(new Text(key));
    this.setDataCategory(dataCategory);
    this.setDataKey(dataKey);
    this.setDataValue(dataValue);
  }

  public JobHistoryRawRecord() {

  }

  public JobHistoryRawRecord(String rawKey) {
    this.setKey(new Text(rawKey));
  }

  public void set(RecordCategory category, RecordDataKey key, String value) {
    this.setDataCategory(category);
    this.setDataKey(key);
    this.setDataValue(value);
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }
}
