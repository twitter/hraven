package com.twitter.hraven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class RecordDataKey implements Writable {
  private List<String> components;

  public RecordDataKey(String... components) {
    this.components = Arrays.asList(components);
  }

  public RecordDataKey(String firstComponent) {
    this.components = new ArrayList<String>();
    this.components.add(firstComponent);
  }

  public void add(String component) {
    this.components.add(component);
  }

  public String get(int index) {
    return components.get(index);
  }

  public List<String> getComponents() {
    return components;
  }

  @Override
  public int hashCode() {
    return 1 + ((components == null) ? 0 : components.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RecordDataKey other = (RecordDataKey) obj;
    if (components == null) {
      if (other.components != null) {
        return false;
      }
    } else if (!components.equals(other.components)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return StringUtils.join(Constants.PERIOD_SEP_CHAR, components);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new ArrayWritable((String[])this.components.toArray()).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ArrayWritable a = new ArrayWritable(Text.class);
    a.readFields(in);
    this.components = Arrays.asList(a.toStrings());
  }
}
