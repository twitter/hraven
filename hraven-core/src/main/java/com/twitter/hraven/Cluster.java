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
package com.twitter.hraven;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Cluster {
  private static Map<String, String> CLUSTERS_BY_HOST = new HashMap<String, String>();

  public static String getIdentifier(String hostname) {
    return CLUSTERS_BY_HOST.get(hostname);
  }

  static {
    // read the property file
    // populate the map
    Properties prop = new Properties();
    try {
      //TODO : property file to be moved out from resources into config dir
      prop.load(Cluster.class.getResourceAsStream("/hadoopclusters.properties"));
      Set<String> hostnames = prop.stringPropertyNames();
      for (String h : hostnames) {
        CLUSTERS_BY_HOST.put(h, prop.getProperty(h));
      }
    } catch (IOException e) {
      // An ExceptionInInitializerError will be thrown to indicate that an
      // exception occurred during evaluation of a static initializer or the
      // initializer for a static variable.
      throw new ExceptionInInitializerError(
          " Could not load properties file hadoopclusters.properties ");
    }
  }
}
