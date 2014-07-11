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

import java.util.Arrays;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import com.google.inject.Module;
import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.modules.LogModule;

import com.twitter.common.net.http.HttpServerDispatch;
import com.twitter.common.stats.Stats;

/**
 * This is the application that launches the REST API
 * It also exposes the metrics collected via the Stats System
 * at http://hostname:portnum/vars or
 * http://hostname:portnum/vars.json
 * These metrics can be collected from a plugin to be fed into
 * to any metric collection system
 */
public class HravenRestServer extends AbstractApplication {
  private static final Log LOG = LogFactory.getLog(HravenRestServer.class);

  @Inject private Lifecycle lifecycle;
  @Inject private HttpServerDispatch httpServer;

  @Override
  public void run() {
    LOG.info("Running");
    Map<String, String> initParams = Maps.newHashMap();
    initParams.put("com.sun.jersey.config.property.packages", "com.twitter.hraven.rest");
    initParams.put("com.sun.jersey.api.json.POJOMappingFeature", "true");

    httpServer.registerHandler("/",
      new com.sun.jersey.spi.container.servlet.ServletContainer(), initParams, false);

    // export a metric that printouts the epoch time this service came up
    // metrics can be viewed at hostname:portnumber/vars or
    // hostname:portnumber/vars.json
    Stats.exportLong("hravenRestService_StartTimestamp", System.currentTimeMillis());

    // await shutdown
    lifecycle.awaitShutdown();
  }

  /**
   * This tells AppLauncher what modules to load
   */
  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
      new HttpModule(),
      new LogModule(),
      new StatsModule()
    );
  }

  public static void main(String[] args) {
    AppLauncher.launch(HravenRestServer.class, args);
  }
}

