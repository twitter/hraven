package com.twitter.hraven;

/**
 * 
 * @author angad.singh
 *
 * {@link JobFileTableMapper} outputs this as key. It corresponds to the
 * Hbase table which was earlier emitted
 */

public enum HravenService {
  JOB_HISTORY_RAW {
    @Override
    public HravenRecord getNewRecord() {
      return new JobHistoryRawRecord();
    }
  },
  JOB_HISTORY {
    @Override
    public HravenRecord getNewRecord() {
      return new JobHistoryRecord();
    }
  },
  JOB_HISTORY_TASK {
    @Override
    public HravenRecord getNewRecord() {
      return new JobHistoryTaskRecord();
    }
  };

  public abstract HravenRecord getNewRecord();
}
