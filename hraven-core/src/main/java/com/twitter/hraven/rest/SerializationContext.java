/*
Copyright 2013 Twitter, Inc.

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
package com.twitter.hraven.rest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
*/
public class SerializationContext {

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(SerializationContext.class);

  public enum DetailLevel {

    /**
     * Indicating that everything in the object should be returned
     */
    EVERYTHING,

    /**
     * Indicating that only summary stats are to be returned
     */
    FLOW_SUMMARY_STATS_ONLY,

    /**
     * Indicating that only queue and run id are to be returned
     * for newJobs
     */
    APP_SUMMARY_STATS_NEW_JOBS_ONLY,

    /**
     * return all apps
     */
    APP_SUMMARY_STATS_ALL_APPS,

    /**
     * Indicating that job details along with summary stats are to be returned
     */
    FLOW_SUMMARY_STATS_WITH_JOB_STATS;
  }

  /**
   * Restricts returned job configuration data to specific configuration
   * properties.
   */
  public static class FieldNameFilter implements Predicate<String> {
    private final Set<String> allowedKeys;

    public FieldNameFilter(List<String> keys) {
      if (keys != null) {
        this.allowedKeys = new HashSet<String>(keys);
      } else {
        this.allowedKeys = null;
      }
    }

    /**
     * Returns <code>true</code> if the given configuration property
     * is contained in the set of allowed configuration keys.
     */
    @Override
    public boolean apply(String potentialKey) {
      return allowedKeys != null && allowedKeys.contains(potentialKey);
    }
  }

  /**
   * Restricts returned job configuration data to configuration properties matching a set
   * of regular expressions.
   */
  public static class RegexConfigurationFilter implements Predicate<String> {
    private final List<Pattern> allowedPatterns;

    public RegexConfigurationFilter(List<String> patterns) {
      if (patterns != null) {
        allowedPatterns = Lists.newArrayListWithCapacity(patterns.size());
        for (String p : patterns) {
          allowedPatterns.add(Pattern.compile(p));
        }
      } else {
        allowedPatterns = null;
      }
    }

    @Override
    public boolean apply(String potentialKey) {
      if (allowedPatterns != null) {
        for (Pattern p : allowedPatterns) {
          if (p.matcher(potentialKey).matches()) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private final DetailLevel level;
  private final Predicate<String> configFilter;
  private final Predicate<String> flowFilter;
  private final Predicate<String> jobFilter;
  private final Predicate<String> counterFilter;
  private final Predicate<String> taskFilter;

  public SerializationContext(DetailLevel serializationLevel) {
    this.level = serializationLevel;
    this.configFilter = null;
    this.flowFilter = null;
    this.jobFilter = null;
    this.counterFilter = null;
    this.taskFilter = null;
  }

  /**
   * constructor to set the config filter, job filter and task filter
   * @param serializationLevel
   * @param configFilter
   * @param jobFilter
   * @param taskFilter
   */
  public SerializationContext(DetailLevel serializationLevel,
                              Predicate<String> configFilter,
                              Predicate<String> flowFilter,
                              Predicate<String> jobFilter,
                              Predicate<String> taskFilter) {
    this.level = serializationLevel;
    this.configFilter = configFilter;
    this.flowFilter = flowFilter;
    this.jobFilter = jobFilter;
    this.taskFilter = taskFilter;
    this.counterFilter = null;
  }

  /**
   * constructor to set the config filter, job filter,
   * task filter & counter filter
   * @param serializationLevel
   * @param configFilter
   * @param jobFilter
   * @param taskFilter
   * @param counterFilter
   */
  public SerializationContext(DetailLevel serializationLevel,
                              Predicate<String> configFilter,
                              Predicate<String> flowFilter,
                              Predicate<String> jobFilter,
                              Predicate<String> taskFilter,
                              Predicate<String> counterFilter) {
    this.level = serializationLevel;
    this.configFilter = configFilter;
    this.flowFilter = flowFilter;
    this.jobFilter = jobFilter;
    this.taskFilter = taskFilter;
    this.counterFilter = counterFilter;
  }

  public DetailLevel getLevel() {
    return this.level;
  }

  public Predicate<String> getConfigurationFilter() {
    return this.configFilter;
  }

  public Predicate<String> getFlowFilter() {
    return flowFilter;
  }

  public Predicate<String> getJobFilter() {
    return jobFilter;
  }

  public Predicate<String> getTaskFilter() {
    return taskFilter;
  }

  public Predicate<String> getCounterFilter() {
    return counterFilter;
  }

}
