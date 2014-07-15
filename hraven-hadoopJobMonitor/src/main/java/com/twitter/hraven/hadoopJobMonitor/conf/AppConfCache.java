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
package com.twitter.hraven.hadoopJobMonitor.conf;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * A bounded cache for {@link AppConfiguraiton}
 */
public class AppConfCache extends
    LinkedHashMap<ApplicationId, AppConfiguraiton> {
  private static final long serialVersionUID = 5272154120725458154L;

  private final int maxEntries;

  private static volatile AppConfCache INSTANCE = null;

  synchronized public static void init(HadoopJobMonitorConfiguration conf) {
    if (INSTANCE != null)
      return;
    int cacheSize =
        conf.getInt(HadoopJobMonitorConfiguration.MAX_CACHED_APP_CONFS,
            HadoopJobMonitorConfiguration.DEFAULT_MAX_CACHED_APP_CONFS);
    INSTANCE = new AppConfCache(cacheSize);
  }

  public static AppConfCache getInstance() {
    return INSTANCE;
  }

  private AppConfCache(final int maxEntries) {
    super(maxEntries + 1, 0.75f, true);
    this.maxEntries = maxEntries;
  }

  @Override
  protected boolean removeEldestEntry(
      final Map.Entry<ApplicationId, AppConfiguraiton> eldest) {
    return super.size() > maxEntries;
  }

}
