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
package com.twitter.hraven.hadoopJobMonitor.jmx;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

/**
 * The interface to manage the white listed apps and their expiration dates
 */
public interface WhiteListMBean {
  /**
   * Get the expiration dates of the current white list
   * @return
   */
  public Map<String,Date> getExpirations();
  /**
   * White list an app until the next N minutes
   * @param appId
   * @param minutes
   * @return
   * @throws Exception 
   */
  public Date expireAfterMinutes(String appId, int minutes) throws Exception;
  public String load() throws FileNotFoundException, IOException;
  public String store() throws Exception;
}
