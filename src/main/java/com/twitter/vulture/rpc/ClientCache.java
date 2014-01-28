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

package com.twitter.vulture.rpc;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

/**
 * Extending {@link org.apache.hadoop.mapred.ClientCache} to add the capability
 * of doing manual garbage collection. The difference is only the method
 * {@link #stopClient(JobID)}.
 * 
 * The way we use ClientCache is not much of a cache.  Thus the name is changed.
 */
public class ClientCache {

  private final Configuration conf;
  private final ResourceMgrDelegate rm;

  private static final Log LOG = LogFactory.getLog(ClientCache.class);

  private Map<JobID, ClientServiceDelegate> cache =
      new HashMap<JobID, ClientServiceDelegate>();

  private MRClientProtocol hsProxy;

  public ClientCache(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  // TODO: evict from the cache on some threshold
  public synchronized ClientServiceDelegate getClient(JobID jobId) {
    if (hsProxy == null) {
      try {
        hsProxy = instantiateHistoryProxy();
      } catch (IOException e) {
        LOG.warn("Could not connect to History server.", e);
        throw new YarnException("Could not connect to History server.", e);
      }
    }
    ClientServiceDelegate client = cache.get(jobId);
    if (client == null) {
      client = new ClientServiceDelegate(conf, rm, jobId, hsProxy);
      cache.put(jobId, client);
    }
    return client;
  }

  protected synchronized MRClientProtocol getInitializedHSProxy()
      throws IOException {
    if (this.hsProxy == null) {
      hsProxy = instantiateHistoryProxy();
    }
    return this.hsProxy;
  }

  protected MRClientProtocol instantiateHistoryProxy() throws IOException {
    final String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);
    if (StringUtils.isEmpty(serviceAddr)) {
      return null;
    }
    LOG.debug("Connecting to HistoryServer at: " + serviceAddr);
    final YarnRPC rpc = YarnRPC.create(conf);
    LOG.debug("Connected to HistoryServer at: " + serviceAddr);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });
  }

  public synchronized void stopClient(JobID jobId) {
    cache.remove(jobId);
  }
}
