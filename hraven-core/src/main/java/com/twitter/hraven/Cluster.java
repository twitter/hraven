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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Cluster {
  private static Map<String, String> CLUSTERS_BY_HOST = new HashMap<String, String>();
  private static Log LOG = LogFactory.getLog(Cluster.class);

  public static String getIdentifier(String hostname) {
    return CLUSTERS_BY_HOST.get(hostname);
  }

  static {
    loadHadoopClustersProps(null);
  }

  // package level visibility to enable
  // testing with different properties file names
  static void loadHadoopClustersProps(String filename) {
    // read the property file
    // populate the map
    Properties prop = new Properties();
    if (StringUtils.isBlank(filename)) {
      filename = Constants.HRAVEN_CLUSTER_PROPERTIES_FILENAME;
    }
    try {
      //TODO : property file to be moved out from resources into config dir
      InputStream inp = Cluster.class.getResourceAsStream("/" + filename);
      if (inp == null) {
        LOG.error(filename
            + " for mapping clusters to cluster identifiers in hRaven does not exist");
        return;
      }
      prop.load(inp);
      Set<String> hostnames = prop.stringPropertyNames();
      for (String h : hostnames) {
        CLUSTERS_BY_HOST.put(h, prop.getProperty(h));
      }
    } catch (IOException e) {
      // An ExceptionInInitializerError will be thrown to indicate that an
      // exception occurred during evaluation of a static initializer or the
      // initializer for a static variable.
      throw new ExceptionInInitializerError(" Could not load properties file " + filename
          + " for mapping clusters to cluster identifiers in hRaven");
    }
  }
}
