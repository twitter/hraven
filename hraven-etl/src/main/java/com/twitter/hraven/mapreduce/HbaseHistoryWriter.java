package com.twitter.hraven.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.datasource.ProcessingException;

public class HbaseHistoryWriter {
  private static Log LOG = LogFactory.getLog(HbaseHistoryWriter.class);
  
  public static void addHistoryPuts(HravenRecord record, Put p) {
    byte[] family = Constants.INFO_FAM_BYTES;

    JobHistoryKeys dataKey = null;
    if (record.getDataKey() != null && record.getDataKey().get(0) != null)
      try {
        dataKey = JobHistoryKeys.valueOf(record.getDataKey().get(0));        
      } catch (IllegalArgumentException e) {
        // some keys other than JobHistoryKeys were added by
        // JobHistoryListener. Ignore this exception.
      }

    if (dataKey == null) {
      byte[] qualifier = null;
      if (record.getDataCategory() == RecordCategory.CONF) {
        byte[] jobConfColumnPrefix =
            Bytes.add(Constants.JOB_CONF_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        qualifier = Bytes.add(jobConfColumnPrefix, Bytes.toBytes(record.getDataKey().toString()));
      } else {
        qualifier = Bytes.toBytes(record.getDataKey().toString().toLowerCase());
      }

      byte[] valueBytes = null;
      
      if (record.getDataValue() instanceof Long) {
        valueBytes = Bytes.toBytes((Long)record.getDataValue());
      } else if (record.getDataValue() instanceof Double) {
        valueBytes = Bytes.toBytes((Double)record.getDataValue());
      } else {
        valueBytes = Bytes.toBytes(record.getDataValue().toString());
      }
      
      Bytes.toBytes(record.getDataValue().toString());
      p.add(family, qualifier, valueBytes);
    } else if (dataKey == JobHistoryKeys.COUNTERS || dataKey == JobHistoryKeys.MAP_COUNTERS
        || dataKey == JobHistoryKeys.REDUCE_COUNTERS) {

      String group = record.getDataKey().get(1);
      String counterName = record.getDataKey().get(2);
      byte[] counterPrefix = null;

      try {
        switch (dataKey) {
        case COUNTERS:
        case TOTAL_COUNTERS:
        case TASK_COUNTERS:
        case TASK_ATTEMPT_COUNTERS:
          counterPrefix = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
          break;
        case MAP_COUNTERS:
          counterPrefix = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        case REDUCE_COUNTERS:
          counterPrefix =
              Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        default:
          throw new IllegalArgumentException("Unknown counter type " + dataKey.toString());
        }
      } catch (IllegalArgumentException iae) {
        throw new ProcessingException("Unknown counter type " + dataKey, iae);
      } catch (NullPointerException npe) {
        throw new ProcessingException("Null counter type " + dataKey, npe);
      }

      byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(group), Constants.SEP_BYTES);
      byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counterName));

      p.add(family, qualifier, Bytes.toBytes((Long) record.getDataValue()));
    } else {
      @SuppressWarnings("rawtypes")
      Class clazz = JobHistoryKeys.KEY_TYPES.get(dataKey);
      byte[] valueBytes = null;

      if (Integer.class.equals(clazz)) {
        valueBytes =
            (Integer) record.getDataValue() == 0 ? Constants.ZERO_INT_BYTES : Bytes
                .toBytes((Integer) record.getDataValue());
      } else if (Long.class.equals(clazz)) {
        valueBytes =
            (Long) record.getDataValue() == 0 ? Constants.ZERO_LONG_BYTES : Bytes
                .toBytes((Long) record.getDataValue());
      } else {
        // keep the string representation by default
        valueBytes = Bytes.toBytes((String) record.getDataValue());
      }
      byte[] qualifier = Bytes.toBytes(dataKey.toString().toLowerCase());
      p.add(family, qualifier, valueBytes);
    }
  }
}
