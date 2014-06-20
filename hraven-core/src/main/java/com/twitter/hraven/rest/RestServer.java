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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.google.common.util.concurrent.AbstractIdleService;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Simple REST server that spawns an embedded Jetty instance to service requests
 * @deprecated in favor of {@link HravenRestServer}
 */
public class RestServer extends AbstractIdleService {
  /** Default TCP port for the server to listen on */
  public static final int DEFAULT_PORT = 8080;
  /** Default IP address for the server to listen on */
  public static final String DEFAULT_ADDRESS = "0.0.0.0";

  private static final Log LOG = LogFactory.getLog(RestServer.class);

  private final String address;
  private final int port;
  private Server server;

  public RestServer(String address, int port) {
    this.address = address;
    this.port = port;
  }

  @Override
  protected void startUp() throws Exception {
    // setup the jetty config
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.packages", "com.twitter.hraven.rest");
    sh.setInitParameter(JSONConfiguration.FEATURE_POJO_MAPPING, "true");

    server = new Server();

    Connector connector = new SelectChannelConnector();
    connector.setPort(this.port);
    connector.setHost(address);

    server.addConnector(connector);

    // TODO: in the future we may want to provide settings for the min and max threads
    // Jetty sets the default max thread number to 250, if we don't set it.
    //
    QueuedThreadPool threadPool = new QueuedThreadPool();
    server.setThreadPool(threadPool);

    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);
    // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");

    // start server
    server.start();
  }

  @Override
  protected void shutDown() throws Exception {
    server.stop();
  }

  private static void printUsage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("bin/hraven rest start", "", opts,
        "To run the REST server, execute bin/hraven rest start|stop [-p <port>]", true);
  }

  public static void main(String[] args) throws Exception {
    // parse commandline options
    Options opts = new Options();
    opts.addOption("p", "port", true, "Port for server to bind to (default 8080)");
    opts.addOption("a", "address", true, "IP address for server to bind to (default 0.0.0.0)");
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(opts, args);
    } catch (ParseException pe) {
      LOG.fatal("Failed to parse arguments", pe);
      printUsage(opts);
      System.exit(1);
    }

    String address = DEFAULT_ADDRESS;
    int port = DEFAULT_PORT;
    if (cmd.hasOption("p")) {
      try {
        port = Integer.parseInt(cmd.getOptionValue("p"));
      } catch (NumberFormatException nfe) {
        LOG.fatal("Invalid integer '"+cmd.getOptionValue("p")+"'", nfe);
        printUsage(opts);
        System.exit(2);
      }
    }
    if (cmd.hasOption("a")) {
      address = cmd.getOptionValue("a");
    }
    RestServer server = new RestServer(address, port);
    server.startUp();
    // run until we're done
  }
}
