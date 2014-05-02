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

package com.twitter.hraven;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Represents the capacity information
 * Presently stores only pool/queue capacity related numbers
 * Also provides functionality for loading different schedulers
 * currently loads fair scheduler to
 * get hadoop1 and hadoop2 Pool/Queue capacity numbers
 * Can be extended to include other capacity related information
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class CapacityDetails {

  private static Log LOG = LogFactory.getLog(CapacityDetails.class);

  private SchedulerTypes schedulerType = SchedulerTypes.FAIR_SCHEDULER;

  private Map<String, FairSchedulerCapacityDetails> fairSchedulerCapacityInfo =
      new HashMap<String, FairSchedulerCapacityDetails>();

  public CapacityDetails() {
  }

  public CapacityDetails(String schedulerType, String fileName) {

    if (StringUtils.isBlank(schedulerType)) {
      LOG.error("Cannot instantiate CapacityDetails, "
          + "scheduler type is blank");
      throw new ProcessingException("Cannot instantiate CapacityDetails, "
          + "scheduler type is blank");
    }
    try {
      this.schedulerType = SchedulerTypes.valueOf(schedulerType);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot instantiate CapacityDetails, " + "scheduler type " + schedulerType
          + " is unknown ");
      throw new ProcessingException("Cannot instantiate CapacityDetails, " + "scheduler type "
          + schedulerType + " is unknown ");
    }

    switch (this.schedulerType) {
    case FIFO_SCHEDULER:
      LOG.error(this.schedulerType + " not supported yet");
      break;
    case CAPACITY_SCHEDULER:
      LOG.error(this.schedulerType + " not supported yet");
      break;
    default:
      // try to load as fair scheduler
    case FAIR_SCHEDULER:
      this.fairSchedulerCapacityInfo = FairSchedulerCapacityDetails.loadFromFairScheduler(fileName);
      break;
    }

  }

  public long getMinResources(String queue) {
    FairSchedulerCapacityDetails fs = this.fairSchedulerCapacityInfo.get(queue);
    if (fs != null) {
      return fs.getMinResources();
    } else {
      return 0L;
    }
  }

  public long getMinMaps(String queue) {
    FairSchedulerCapacityDetails fs = this.fairSchedulerCapacityInfo.get(queue);
    if (fs != null) {
      return fs.getMinMaps();
    } else {
      return 0L;
    }
  }

  public long getMinReduces(String queue) {
    FairSchedulerCapacityDetails fs = this.fairSchedulerCapacityInfo.get(queue);
    if (fs != null) {
      return fs.getMinReduces();
    } else {
      return 0L;
    }
  }

  public int size() {
    if (this.schedulerType != null) {
      switch (this.schedulerType) {
      case FIFO_SCHEDULER:
        LOG.error(this.schedulerType + " not supported yet");
        break;
      case CAPACITY_SCHEDULER:
        LOG.error(this.schedulerType + " not supported yet");
        break;
      default:
        // try to load as fair scheduler
      case FAIR_SCHEDULER:
        return this.fairSchedulerCapacityInfo.size();
      }
    }
    return 0;
  }
}