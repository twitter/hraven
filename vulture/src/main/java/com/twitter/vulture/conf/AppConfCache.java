package com.twitter.vulture.conf;

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

  synchronized public static void init(VultureConfiguration conf) {
    if (INSTANCE != null)
      return;
    int cacheSize =
        conf.getInt(VultureConfiguration.MAX_CACHED_APP_CONFS,
            VultureConfiguration.DEFAULT_MAX_CACHED_APP_CONFS);
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
