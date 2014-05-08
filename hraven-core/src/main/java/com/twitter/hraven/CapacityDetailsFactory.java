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

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Represents the factory used to create a CapacityDetails object
 * to store capacity information
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class CapacityDetailsFactory {

  private static Log LOG = LogFactory.getLog(CapacityDetailsFactory.class);

  public static CapacityDetails getCapacityDetails(String schedulerType, String fileName) {

    if (StringUtils.isBlank(schedulerType)) {
      LOG.error("Cannot instantiate CapacityDetails, "
          + "scheduler type is blank");
      throw new ProcessingException("Cannot instantiate CapacityDetails, "
          + "scheduler type is blank");
    }
    SchedulerTypes st;
    try {
      st = SchedulerTypes.valueOf(schedulerType);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot instantiate CapacityDetails, " + "scheduler type " + schedulerType
          + " is unknown ");
      throw new ProcessingException("Cannot instantiate CapacityDetails, " + "scheduler type "
          + schedulerType + " is unknown ");
    }

    switch (st) {
    case FIFO_SCHEDULER:
      LOG.error(schedulerType + " not supported yet");
      break;
    case CAPACITY_SCHEDULER:
      LOG.error(schedulerType + " not supported yet");
      break;
    default:
      // try to load as fair scheduler
    case FAIR_SCHEDULER:
      CapacityDetails capacityInfo = new FairSchedulerCapacityDetails();
      capacityInfo.loadDetails(fileName);
      return capacityInfo;
    }
    return null;
  }
}
