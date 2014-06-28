package com.twitter.hraven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author angad.singh Store multiple {@link JobHistoryRecord}s in a 2 level HashMap Supports
 *         iteration to get individual {@link JobHistoryRecord}s
 */

public class JobHistoryRecordCollection extends HravenRecord<JobKey, Object> implements
    Collection<JobHistoryRecord> {

  private Map<RecordCategory, Map<RecordDataKey, Object>> valueMap;

  public JobHistoryRecordCollection() {
    valueMap = new HashMap<RecordCategory, Map<RecordDataKey, Object>>();
  }

  public JobHistoryRecordCollection(JobKey jobKey) {
    setKey(jobKey);
    valueMap = new HashMap<RecordCategory, Map<RecordDataKey, Object>>();
  }

  public JobHistoryRecordCollection(JobHistoryRecord record) {
    valueMap = new HashMap<RecordCategory, Map<RecordDataKey, Object>>();
    setKey(record.getKey());
    setSubmitTime(record.getSubmitTime());
    add(record);
  }

  public boolean add(RecordCategory category, RecordDataKey key, Object value) {
    if (valueMap.containsKey(category)) {
      valueMap.get(category).put(key, value);
    } else {
      HashMap<RecordDataKey, Object> categoryMap = new HashMap<RecordDataKey, Object>();
      valueMap.put(category, categoryMap);
      categoryMap.put(key, value);
    }
    
    return true;
  }

  public boolean add(JobHistoryRecord record) {
    return add(record.getDataCategory(), record.getDataKey(), record.getDataValue());
  }

  public Map<RecordCategory, Map<RecordDataKey, Object>> getValueMap() {
    return valueMap;
  }

  public Object getValue(RecordCategory category, RecordDataKey key) {
    return valueMap.containsKey(category) ? valueMap.get(category).get(key) : null;
  }

  public int size() {
    int size = 0;
    for (Entry<RecordCategory, Map<RecordDataKey, Object>> catMap : valueMap.entrySet()) {
      size += catMap.getValue().size();
    }

    return size;
  }

  /**
   * Be able to iterate easily to get individual {@link JobHistoryRecord}s
   */

  @Override
  public Iterator<JobHistoryRecord> iterator() {

    return new Iterator<JobHistoryRecord>() {

      private Iterator<Entry<RecordCategory, Map<RecordDataKey, Object>>> catIterator;
      private Iterator<Entry<RecordDataKey, Object>> dataIterator;
      Entry<RecordCategory, Map<RecordDataKey, Object>> nextCat;
      Entry<RecordDataKey, Object> nextData;

      {
        initIterators();
      }

      private void initIterators() {
        if (catIterator == null) {
          catIterator = valueMap.entrySet().iterator();
        }

        if (catIterator.hasNext()) {
          nextCat = catIterator.next();
          dataIterator = nextCat.getValue().entrySet().iterator();
        }
      }

      @Override
      public boolean hasNext() {
        if (dataIterator == null) {
          initIterators();
        }
        return dataIterator == null ? false : dataIterator.hasNext() || catIterator.hasNext();
      }

      @Override
      public JobHistoryRecord next() {
        if (!dataIterator.hasNext()) {
          nextCat = catIterator.next();
          dataIterator = nextCat.getValue().entrySet().iterator();
        }

        nextData = dataIterator.next();

        return new JobHistoryRecord(nextCat.getKey(), getKey(), nextData.getKey(),
            nextData.getValue(), getSubmitTime());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
  }

  public void mergeWith(JobHistoryRecordCollection confRecord) {
    for (JobHistoryRecord record : confRecord) {
      add(record);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((valueMap == null) ? 0 : valueMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JobHistoryRecordCollection other = (JobHistoryRecordCollection) obj;
    if (valueMap == null) {
      if (other.valueMap != null) {
        return false;
      }
    } else if (!valueMap.equals(other.valueMap)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "JobHistoryRecordCollection [key=" + getKey() + ", dataCategory=" + getDataCategory() + ", dataKey=" + getDataKey()
        + ", dataValue=" + getDataValue() + ", submitTime=" + getSubmitTime() + ", valueMap=" + valueMap + "]";
  }

  @Override
  public boolean isEmpty() {
    return valueMap.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends JobHistoryRecord> recordCol) {
    for (JobHistoryRecord record : recordCol) {
      add(record);
    }
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    this.valueMap = null;
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    
    out.writeInt(this.size());
    for (JobHistoryRecord r: this) {
      r.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    
    int size = in.readInt();
    int i = 0;
    while (i < size) {
      JobHistoryRecord r = new JobHistoryRecord();
      r.readFields(in);
      this.add(r);
      i++;
    }
  }
}