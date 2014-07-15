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
package com.twitter.hraven.hadoopJobMonitor.policy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;

import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;

/**
 * A bounded cache for {@link ProgressCache.Progress}
 */
public class ProgressCache<K> extends LinkedHashMap<K, ProgressCache.Progress> {
  private static final long serialVersionUID = 5272154120725458154L;

  private final int maxEntries;

  private static volatile Map<TaskID, ProgressCache.Progress> TASK_INSTANCE =
      null;
  private static volatile Map<TaskAttemptID, ProgressCache.Progress> ATTEMPT_INSTANCE =
      null;

  synchronized public static void init(HadoopJobMonitorConfiguration conf) {
    if (TASK_INSTANCE != null)
      return;
    int cacheSize =
        conf.getInt(HadoopJobMonitorConfiguration.MAX_CACHED_TASK_PROGRESSES,
            HadoopJobMonitorConfiguration.DEFAULT_MAX_CACHED_TASK_PROGRESSES);
    TASK_INSTANCE =
        Collections.synchronizedMap(new ProgressCache<TaskID>(cacheSize));
    ATTEMPT_INSTANCE =
        Collections
            .synchronizedMap(new ProgressCache<TaskAttemptID>(cacheSize));
  }

  public static Map<TaskID, ProgressCache.Progress> getTaskProgressCache() {
    return TASK_INSTANCE;
  }

  public static Map<TaskAttemptID, ProgressCache.Progress> getAttemptProgressCache() {
    return ATTEMPT_INSTANCE;
  }

  private ProgressCache(final int maxEntries) {
    super(maxEntries + 1, 0.75f, true);
    this.maxEntries = maxEntries;
  }

  @Override
  protected boolean removeEldestEntry(
      final Map.Entry<K, ProgressCache.Progress> eldest) {
    return super.size() > maxEntries;
  }

  public static class Progress {
    /**
     * The absolute amount of progress
     */
    public float value;
    /**
     * The last retrieval date in ms
     */
    public long dateMs;

    public Progress(float value, long dateMs) {
      this.value = value;
      this.dateMs = dateMs;
    }
  }

}
