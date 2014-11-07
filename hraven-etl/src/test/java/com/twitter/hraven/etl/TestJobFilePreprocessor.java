package com.twitter.hraven.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.datasource.HRavenTestUtil;

public class TestJobFilePreprocessor {
  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);
  }


  @Test
  public void testSparkProcessRecordCreation() throws IOException{

    final String procDir = "spark_processing_dir";
    final String inputDir = "spark_input_dir";
    final String cluster = "cluster1";

    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    Path inputPath = new Path(inputDir);
    boolean os = hdfs.mkdirs(inputPath);
    assertTrue(os);
    assertTrue(hdfs.exists(inputPath));

    Path procPath = new Path(procDir);
    os = hdfs.mkdirs(procPath);
    assertTrue(os);
    assertTrue(hdfs.exists(procPath));
    hdfs.getFileStatus(procPath);
    
    final String SPARK_JOB_HISTORY_FILE_NAME =
        "src/test/resources/spark_1413515656084_3051855";
    File jobHistoryfile = new File(SPARK_JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    Path expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));

    final String JOB_CONF_FILE_NAME =
        "src/test/resources/spark_1413515656084_3051855_conf.xml";
    File jobConfFile = new File(JOB_CONF_FILE_NAME);
    srcPath = new Path(jobConfFile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));

    JobFilePreprocessor jfpp = new JobFilePreprocessor(UTIL.getConfiguration());
    String args[] = new String[3];
    args[0] = "-o" + procDir;
    args[1] = "-i" + inputDir;
    args[2] = "-c" + cluster;
    try {
      // list the files in processing dir and assert that it's empty
      FileStatus[] fs = hdfs.listStatus(procPath);
      assertEquals(0, fs.length);
      jfpp.run(args);
      // now check if the seq file exists
      // can't check for exact filename since it includes the current timestamp
      fs = hdfs.listStatus(procPath);
      assertEquals(1, fs.length);
      // ensure that hbase table contains the process record
      ProcessRecordService processRecordService = new ProcessRecordService(
        UTIL.getConfiguration());
      ProcessRecord pr = processRecordService.getLastSuccessfulProcessRecord(cluster);
      assertNotNull(pr);
      assertEquals(pr.getMaxJobId(), pr.getMinJobId());
      assertEquals("spark_1413515656084_3051855", pr.getMaxJobId());
      ProcessRecordKey pk = pr.getKey();
      assertNotNull(pk);
      assertEquals(cluster, pk.getCluster());
      assertEquals(2, pr.getProcessedJobFiles());
      assertEquals(ProcessState.PREPROCESSED, pr.getProcessState());
      assertEquals(fs[0].getPath().getParent().getName()
        + "/" + fs[0].getPath().getName(), pr.getProcessFile());

      // run raw loader as well
      args = new String[2];
      args[0] = "-p" + procDir;
      args[1] = "-c" + cluster;
      JobFileRawLoader jr = new JobFileRawLoader(UTIL.getConfiguration());
      assertNotNull(jr);
      ToolRunner.run(jr, args);
      pr = processRecordService.getLastSuccessfulProcessRecord(cluster);
      assertEquals(ProcessState.LOADED, pr.getProcessState());
    }
      catch (Exception e) {
      e.printStackTrace();
    }
  }
  
}
