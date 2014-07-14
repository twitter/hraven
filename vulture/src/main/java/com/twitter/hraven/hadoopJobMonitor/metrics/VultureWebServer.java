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
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.hraven.hadoopJobMonitor.metrics;

import java.io.IOException;
import java.net.InetAddress;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.google.inject.Singleton;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * A very simple web interface for the metric reported by {@link VultureMetrics}
 */
public class VultureWebServer {
  private static final Log LOG = LogFactory.getLog(VultureWebServer.class);

  private WebApp webApp;
  private int port;
  private String host;
  private String webAddress;

  public String getWebAddress() {
    return this.webAddress;
  }

  public String getHostName() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public void start(int port) throws IOException {
    LOG.info("Instantiating " + VultureWebApp.class.getName());
    try {
      VultureWebApp vultureWebApp = new VultureWebApp();
      this.webApp = WebApps.$for("node").at(port).start(vultureWebApp);
      this.port = this.webApp.httpServer().getPort();
      this.host = InetAddress.getLocalHost().getHostName();
      this.webAddress = this.host + ":" + port;
      LOG.info(VultureWebApp.class.getName() + " started: " + this.webAddress);
    } catch (Exception e) {
      String msg = VultureWebApp.class.getName() + " failed to start.";
      LOG.error(msg, e);
      throw new IOException(msg);
    }
  }

  public void stop() {
    if (this.webApp != null) {
      this.webApp.stop();
    }
  }

  private static class VultureWebApp extends WebApp {
    @Override
    public void setup() {
      bind(VultureWebService.class);
      serve("/xml/metrics").with(GuiceContainer.class);
    }
  }

  @Singleton
  @Path("/xml")
  public static class VultureWebService {
    private @Context
    HttpServletResponse response;

    public VultureWebService() {
    }

    private void init() {
      // clear content type
      response.setContentType(null);
    }

    @GET
    @Path("/metrics")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public VultureMetrics getVultureMetricsInfo() {
      init();
      return VultureMetrics.getInstance();
    }

  }

}
