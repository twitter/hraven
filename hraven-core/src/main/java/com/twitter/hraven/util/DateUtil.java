/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.hraven.util;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 */
public class DateUtil {
  public static final long MONTH_IN_MILLIS = 30L*24*60*60*1000;

  /**
   * @return the timestamp (in milliseconds) of baseTimestamp truncate to month start
   */
  public static long getMonthStart(long baseTimestamp) {
    Calendar cal = new GregorianCalendar();
    cal.setTimeInMillis(baseTimestamp);
    // truncate to start of month
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();
  }
}
