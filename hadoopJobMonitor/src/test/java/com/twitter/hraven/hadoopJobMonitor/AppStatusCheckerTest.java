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
package com.twitter.hraven.hadoopJobMonitor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.twitter.hraven.hadoopJobMonitor.AppCheckerProgress;
import com.twitter.hraven.hadoopJobMonitor.AppStatusChecker;
import com.twitter.hraven.hadoopJobMonitor.conf.AppConfCache;
import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton;
import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;
import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton.ConfigurationAccessException;
import com.twitter.hraven.hadoopJobMonitor.metrics.HadoopJobMonitorMetrics;
import com.twitter.hraven.hadoopJobMonitor.policy.ProgressCache;
import com.twitter.hraven.hadoopJobMonitor.policy.ProgressCache.Progress;
import com.twitter.hraven.hadoopJobMonitor.rpc.ClientCache;
import com.twitter.hraven.hadoopJobMonitor.rpc.RestClient;
import com.twitter.hraven.hadoopJobMonitor.rpc.RestClient.RestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest( { RestClient.class})
public class AppStatusCheckerTest {
  private static RecordFactory recordFactory;
  static long MIN = 60 * 1000;

  static {
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
  }
  
  class MyApplicationId extends ApplicationId {
    int id;
    long clusterTimestamp;
    @Override
    public void setId(int id) {
      this.id = id;
    }

    @Override
    public void setClusterTimestamp(long clusterTimestamp) {
      this.clusterTimestamp = clusterTimestamp;
    }

    @Override
    public int getId() {
      return id;
    }

    @Override
    public long getClusterTimestamp() {
      return clusterTimestamp;
    }

    @Override
    protected void build() {
    }
  }
  
  private JobID oldJobId = JobID.forName("job_1315895242400_2");
  private MyApplicationId appId;
  private TaskID taskId;
  private TaskAttemptID taskAttemptId;
  HadoopJobMonitorConfiguration vConf = new HadoopJobMonitorConfiguration();
  ClientCache clientCache = mock(ClientCache.class);
  ClientServiceDelegate clientService = mock(ClientServiceDelegate.class);
  ApplicationReport appReport;
  RestClient restClient;
  long now = System.currentTimeMillis();
  ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class); 
  AppStatusChecker appStatusChecker;
  Map<org.apache.hadoop.mapred.TaskID, Progress>  taskProgressCache;
  Map<TaskAttemptID, Progress>  attemptProgressCache;

  public AppStatusCheckerTest() throws ConfigurationAccessException, RestException, SAXException, IOException, ParserConfigurationException, YarnException {
    appId = new MyApplicationId();
    appId.setId(oldJobId.getId());
    appId.setClusterTimestamp(Long.parseLong(oldJobId.getJtIdentifier()));  
    
    taskId = new TaskID(oldJobId, TaskType.MAP, 0);
    taskAttemptId = new TaskAttemptID(taskId, 0);
    
    vConf.setFloat(HadoopJobMonitorConfiguration.TASK_PROGRESS_THRESHOLD, 0.2f);
    vConf.getInt(HadoopJobMonitorConfiguration.MAX_CACHED_TASK_PROGRESSES,10);
    vConf.getInt(HadoopJobMonitorConfiguration.MAX_CACHED_APP_CONFS,10);
    AppConfCache.init(vConf);
    ProgressCache.init(vConf);
    HadoopJobMonitorMetrics.initSingleton(vConf);
    taskProgressCache = ProgressCache.getTaskProgressCache();
    attemptProgressCache = ProgressCache.getAttemptProgressCache();
    
    when(clientCache.getClient(any(JobID.class))).thenReturn(clientService);
    appReport = mock(ApplicationReport.class);
    when(appReport.getApplicationId()).thenReturn(appId);
    appStatusChecker = new AppStatusChecker(vConf, appReport, clientCache, rm,
        new AppCheckerProgress() {
          @Override
          public void finished() {
          }
    });
    
    mockStatic(RestClient.class);
    restClient = mock(RestClient.class);
    when(RestClient.getInstance()).thenReturn(restClient);
  }
  
  void setTaskAttemptXML(long elapsedTime, float progress) throws Exception {
    String xmlString =
        "<taskAttempt><elapsedTime>" + elapsedTime + "</elapsedTime>"
            + "<progress>" + progress + "</progress>" + "</taskAttempt>";
    Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder()
            .parse(new InputSource(new StringReader(xmlString)));
    when(restClient.getXml(any(String.class))).thenReturn(doc);
  }
  
  int killCounter;
  @Test
  public void testMapTasks() throws Exception {
    killCounter = 0;
    final String pName = HadoopJobMonitorConfiguration.MAP_MAX_RUNTIME_MIN;
    final boolean passCheck = true, killed = true, dryRun = true, enforce = true;
    testTask(TaskType.MAP, pName, 5, 10, enforce, !dryRun, TIPStatus.RUNNING, passCheck, !killed);
    testTask(TaskType.MAP, pName, 15, 10, enforce, !dryRun, TIPStatus.FAILED, passCheck, !killed);
    testTask(TaskType.MAP, pName, 15, 10, enforce, !dryRun, TIPStatus.RUNNING, !passCheck, killed);
    testTask(TaskType.MAP, pName, 15, 10, !enforce, !dryRun, TIPStatus.RUNNING, !passCheck, !killed);
    testTask(TaskType.MAP, pName, 15, 10, !enforce, dryRun, TIPStatus.RUNNING, !passCheck, !killed);
    testTask(TaskType.MAP, pName, 15, 10, enforce, dryRun, TIPStatus.RUNNING, !passCheck, !killed);
  }

  @Test
  public void testReduceTasks() throws Exception {
    killCounter = 0;
    final String pName = HadoopJobMonitorConfiguration.REDUCE_MAX_RUNTIME_MIN;
    final boolean passCheck = true, killed = true, dryRun = true, enforce = true;
    testTask(TaskType.REDUCE, pName, 5, 10, enforce, !dryRun, TIPStatus.RUNNING, passCheck, !killed);
    testTask(TaskType.REDUCE, pName, 5, 10, 0.01f, enforce, !dryRun, TIPStatus.RUNNING, passCheck, !killed);
    testTask(TaskType.REDUCE, pName, 15, 10, enforce, !dryRun, TIPStatus.FAILED, passCheck, !killed);
    testTask(TaskType.REDUCE, pName, 15, 10, enforce, !dryRun, TIPStatus.RUNNING, !passCheck, killed);
    testTask(TaskType.REDUCE, pName, 15, 10, !enforce, !dryRun, TIPStatus.RUNNING, !passCheck, !killed);
    testTask(TaskType.REDUCE, pName, 15, 10, !enforce, dryRun, TIPStatus.RUNNING, !passCheck, !killed);
    testTask(TaskType.REDUCE, pName, 15, 10, enforce, dryRun, TIPStatus.RUNNING, !passCheck, !killed);
  }

  @Test
  public void testReduceProgress() throws Exception {
    testProgress(TaskType.REDUCE, HadoopJobMonitorConfiguration.REDUCE_MAX_RUNTIME_MIN);
  }
  
  @Test
  public void testMapProgress() throws Exception {
    testProgress(TaskType.MAP, HadoopJobMonitorConfiguration.MAP_MAX_RUNTIME_MIN);
  }
  
  public void testProgress(TaskType taskType, String pName) throws Exception {
    killCounter = 0;
    final boolean passCheck = true, killed = true, dryRun = true, enforce = true;

    float prevProgress = 0.2f;
    taskProgressCache.put(org.apache.hadoop.mapred.TaskID.downgrade(taskId), new Progress(prevProgress, now - 4 * MIN));
    attemptProgressCache.put(taskAttemptId, new Progress(prevProgress, now - 4 * MIN));
    //from now -4 until now expected progress is 0.4f, and threshold is set to 0.2f
    testTask(taskType, pName, 5, 10, prevProgress + 0.01f, enforce, !dryRun, TIPStatus.RUNNING, !passCheck, killed);
    taskProgressCache.clear();
    attemptProgressCache.clear();
    taskProgressCache.put(org.apache.hadoop.mapred.TaskID.downgrade(taskId), new Progress(prevProgress, now - 4 * MIN));
    attemptProgressCache.put(taskAttemptId, new Progress(prevProgress, now - 4 * MIN));
    testTask(taskType, pName, 5, 10, prevProgress + 0.01f, !enforce, !dryRun, TIPStatus.RUNNING, !passCheck, !killed);
    taskProgressCache.clear();
    attemptProgressCache.clear();
    taskProgressCache.put(org.apache.hadoop.mapred.TaskID.downgrade(taskId), new Progress(prevProgress, now - 4 * MIN));
    attemptProgressCache.put(taskAttemptId, new Progress(prevProgress, now - 4 * MIN));
    testTask(taskType, pName, 5, 10, prevProgress + 0.21f, enforce, !dryRun, TIPStatus.RUNNING, passCheck, !killed);
  }

  public boolean testTask(TaskType taskType, String confParamName,
      long durationMin, final int MAX_RUN, boolean enforce, boolean dryRun,
      TIPStatus status, boolean wellBahaved, boolean killed) throws Exception {
    float progress = Math.max(1, durationMin / (float) MAX_RUN);
    return testTask(taskType, confParamName, durationMin, MAX_RUN, progress,
        enforce, dryRun, status, wellBahaved, killed);
  }
  
  public boolean testTask(TaskType taskType, String confParamName,
      long durationMin, final int MAX_RUN, float progress, boolean enforce,
      boolean dryRun, TIPStatus status, boolean wellBahaved, boolean killed)
      throws Exception {
    setTaskAttemptXML(durationMin * MIN, progress);

    TaskReport taskReport = mock(TaskReport.class);
    when(taskReport.getCurrentStatus()).thenReturn(status);
    Collection<TaskAttemptID> attempts = new ArrayList<TaskAttemptID>();
    attempts.add(taskAttemptId);
    when(taskReport.getRunningTaskAttemptIds()).thenReturn(attempts);
    when(taskReport.getTaskID()).thenReturn(org.apache.hadoop.mapred.TaskID.downgrade(taskId));
    when(taskReport.getProgress()).thenReturn(progress);
    
    vConf.setBoolean(HadoopJobMonitorConfiguration.DRY_RUN, dryRun);
    Configuration remoteAppConf = new Configuration();
    remoteAppConf.setInt(confParamName, MAX_RUN);
    remoteAppConf.setBoolean(HadoopJobMonitorConfiguration.enforced(confParamName), enforce);
    when(taskReport.getStartTime()).thenReturn(now - durationMin * MIN);
    AppConfiguraiton appConf = new AppConfiguraiton(remoteAppConf, vConf);
    AppConfCache.getInstance().put(appId, appConf);
    appStatusChecker.init();
    appStatusChecker.loadClientService();
    
    boolean res = appStatusChecker.checkTask(taskType, taskReport, now);
    
    if (wellBahaved)
      assertEquals("Well-bahved task does not pass the check", wellBahaved, res);
    else
      assertEquals("Not Well-bahved task passes the check", wellBahaved, res);
    if (killed) {
      killCounter++;
      verify(clientService, times(killCounter)).killTask(any(TaskAttemptID.class), Mockito.anyBoolean());
    } else
      verify(clientService, times(killCounter)).killTask(any(TaskAttemptID.class), Mockito.anyBoolean());
    return res;
}

  @Test
  public void testUnsetEnforce() throws IOException, ConfigurationAccessException {
    Configuration remoteAppConf = new Configuration();
    remoteAppConf.setInt(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN, 10);
    //remoteAppConf.setBoolean(HadoopJobMonitorConfiguration.enforced(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN), true);
    when(appReport.getStartTime()).thenReturn(now - 15 * MIN);

    AppConfiguraiton appConf = new AppConfiguraiton(remoteAppConf, vConf);
    AppConfCache.getInstance().put(appId, appConf);
    appStatusChecker.init();
    
    boolean res = appStatusChecker.checkApp();
    Assert.assertTrue("fails job duration check even though enforce is not set", res);
  }
  
  @Test
  public void testLongJobDryRun() throws IOException, ConfigurationAccessException, YarnException {
    Configuration remoteAppConf = new Configuration();
    remoteAppConf.setInt(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN, 10);
    remoteAppConf.setBoolean(HadoopJobMonitorConfiguration.enforced(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN), true);
    when(appReport.getStartTime()).thenReturn(now - 15 * MIN);

    AppConfiguraiton appConf = new AppConfiguraiton(remoteAppConf, vConf);
    AppConfCache.getInstance().put(appId, appConf);
    appStatusChecker.init();
    
    boolean res = appStatusChecker.checkApp();
    Assert.assertFalse("does not fail job duration check even though enforce is set", res);
    verify(rm, times(0)).killApplication(appId);
  }
  
  @Test
  public void testLongJob() throws IOException, ConfigurationAccessException, YarnException {
    Configuration remoteAppConf = new Configuration();
    remoteAppConf.setInt(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN, 10);
    remoteAppConf.setBoolean(HadoopJobMonitorConfiguration.enforced(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN), true);
    when(appReport.getStartTime()).thenReturn(now - 15 * MIN);
    vConf.setBoolean(HadoopJobMonitorConfiguration.DRY_RUN, false);

    AppConfiguraiton appConf = new AppConfiguraiton(remoteAppConf, vConf);
    AppConfCache.getInstance().put(appId, appConf);
    appStatusChecker.init();
    
    boolean res = appStatusChecker.checkApp();
    Assert.assertFalse("does not fail job duration check even though enforce is set", res);
    verify(rm, times(1)).killApplication(appId);
  }
  
  @Test
  public void testShortJob() throws IOException, ConfigurationAccessException {
    Configuration remoteAppConf = new Configuration();
    remoteAppConf.setInt(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN, 10);
    remoteAppConf.setBoolean(HadoopJobMonitorConfiguration.enforced(HadoopJobMonitorConfiguration.JOB_MAX_LEN_MIN), true);
    when(appReport.getStartTime()).thenReturn(now - 5 * MIN);

    AppConfiguraiton appConf = new AppConfiguraiton(remoteAppConf, vConf);
    AppConfCache.getInstance().put(appId, appConf);
    appStatusChecker.init();
    
    boolean res = appStatusChecker.checkApp();
    Assert.assertTrue("fails job duration check even though the job is not too long", res);
  }
  
}
