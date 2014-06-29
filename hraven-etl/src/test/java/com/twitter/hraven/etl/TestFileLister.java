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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.datasource.ProcessingException;

public class TestFileLister {
  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
  }

  @Test
  public void testPruneFileListBySize() throws IOException {

    long maxFileSize = 20L;
    FileStatus[] origList = new FileStatus[2];
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    Path inputPath = new Path("/inputdir_filesize");
    boolean os = hdfs.mkdirs(inputPath);
    assertTrue(os);
    assertTrue(hdfs.exists(inputPath));

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    Path expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));
    origList[0] = hdfs.getFileStatus(expPath);

    final String JOB_CONF_FILE_NAME =
        "src/test/resources/job_1329348432655_0001_conf.xml";
    File jobConfFile = new File(JOB_CONF_FILE_NAME);
    srcPath = new Path(jobConfFile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));
    origList[1] = hdfs.getFileStatus(expPath);

    FileStatus [] prunedList = FileLister.pruneFileList(maxFileSize, origList);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 0);

    Path emptyFile = new Path(inputPath.toUri() + "/" + "job_1329341111111_0101-1329111113227-user2-Sleep.jhist");
    os = hdfs.createNewFile(emptyFile);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyFile));
    origList[0] = hdfs.getFileStatus(emptyFile);

    Path emptyConfFile = new Path(inputPath.toUri() + "/" + "job_1329341111111_0101_conf.xml");
    os = hdfs.createNewFile(emptyConfFile);

    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile));
    origList[1] = hdfs.getFileStatus(emptyConfFile);

    prunedList = FileLister.pruneFileList(maxFileSize, origList);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 2);

  }

  /**
   * removes conf file which has already been put in prunedList
   *
   * @throws IOException
   */
  @Test
  public void testPruneFileListRemovingConfFromPruneList() throws IOException {

    long maxFileSize = 20L;
    FileStatus[] origList = new FileStatus[2];
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    Path inputPath = new Path("/inputdir_filesize_pruneList");
    boolean os = hdfs.mkdirs(inputPath);
    assertTrue(os);
    assertTrue(hdfs.exists(inputPath));

    Path relocationPath = new Path("/relocation_filesize_pruneList");
    os = hdfs.mkdirs(relocationPath);
    assertTrue(os);
    assertTrue(hdfs.exists(relocationPath));

    Path emptyConfFile = new Path(inputPath.toUri() + "/" + "job_1329348432655_0001_conf.xml");
    os = hdfs.createNewFile(emptyConfFile);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile));
    origList[0] = hdfs.getFileStatus(emptyConfFile);

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    Path expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));
    origList[1] = hdfs.getFileStatus(expPath);

    FileStatus [] prunedList = FileLister.pruneFileList(maxFileSize, origList);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 0);
  }

  /**
   * tests the case when several files are spread out in the dir and need to be removed
   *
   * @throws IOException
   */
  @Test
  public void testPruneFileListMultipleFilesAlreadyMovedCases() throws IOException {

    long maxFileSize = 20L;
    FileStatus[] origList = new FileStatus[12];
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    Path inputPath = new Path("/inputdir_filesize_multiple");
    boolean os = hdfs.mkdirs(inputPath);
    assertTrue(os);
    assertTrue(hdfs.exists(inputPath));

    Path relocationPath = new Path("/relocation_filesize_multiple");
    os = hdfs.mkdirs(relocationPath);
    assertTrue(os);
    assertTrue(hdfs.exists(relocationPath));

    Path emptyFile = new Path(inputPath.toUri() + "/" + "job_1329341111111_0101-1329111113227-user2-Sleep.jhist");
    os = hdfs.createNewFile(emptyFile);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyFile));
    origList[0] = hdfs.getFileStatus(emptyFile);

    Path emptyConfFile = new Path(inputPath.toUri() + "/" + "job_1329341111111_0101_conf.xml");
    os = hdfs.createNewFile(emptyConfFile);

    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile));
    origList[1] = hdfs.getFileStatus(emptyConfFile);

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    Path expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));
    origList[2] = hdfs.getFileStatus(expPath);

    final String JOB_CONF_FILE_NAME =
        "src/test/resources/job_1329348432655_0001_conf.xml";
    File jobConfFile = new File(JOB_CONF_FILE_NAME);
    srcPath = new Path(jobConfFile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputPath);
    expPath = new Path(inputPath.toUri() + "/" + srcPath.getName());
    assertTrue(hdfs.exists(expPath));
    origList[3] = hdfs.getFileStatus(expPath);

    Path inputPath2 = new Path(inputPath.toUri() + "/" + "job_1311222222255_0221-1311111143227-user10101-WordCount-1-SUCCEEDED-default.jhist");
    hdfs.copyFromLocalFile(srcPath, inputPath2);
    assertTrue(hdfs.exists(inputPath2));
    origList[4] = hdfs.getFileStatus(inputPath2);

    Path inputPath3 = new Path(inputPath.toUri() + "/" + "job_1399999999155_0991-1311111143227-user3321-TeraGen-1-SUCCEEDED-default.jhist");
    hdfs.copyFromLocalFile(srcPath, inputPath3);
    assertTrue(hdfs.exists(inputPath3));
    origList[5] = hdfs.getFileStatus( inputPath3);

    Path inputPath4 = new Path(inputPath.toUri() + "/" + "job_1399977777177_0771-1311111143227-user3321-TeraSort-1-SUCCEEDED-default.jhist");
    hdfs.copyFromLocalFile(srcPath, inputPath4);
    assertTrue(hdfs.exists(inputPath4));
    origList[6] = hdfs.getFileStatus( inputPath4);

    Path emptyFile2 = new Path(inputPath.toUri() + "/" + "job_1329343333333_5551-1329111113227-user2-SomethingElse.jhist");
    os = hdfs.createNewFile(emptyFile2);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyFile2));
    origList[7] = hdfs.getFileStatus(emptyFile2);

    Path emptyConfFile2 = new Path(inputPath.toUri() + "/" + "job_1329343333333_5551_conf.xml");
    os = hdfs.createNewFile(emptyConfFile2);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile2));
    origList[8] = hdfs.getFileStatus(emptyConfFile2);

    // this is an empty file which tests the toBeRemovedFileList
    // at the end of function pruneFileListBySize
    Path emptyConfFile3 = new Path(inputPath.toUri() + "/" + "job_1399999999155_0991_conf.xml");
    os = hdfs.createNewFile(emptyConfFile3);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile3));
    origList[9] = hdfs.getFileStatus(emptyConfFile3);

    Path inputConfPath2 = new Path(inputPath.toUri() + "/" + "job_1311222222255_0221_conf.xml");
    srcPath = new Path(jobConfFile.toURI());
    hdfs.copyFromLocalFile(srcPath, inputConfPath2);
    assertTrue(hdfs.exists(inputConfPath2));
    origList[10] = hdfs.getFileStatus(inputConfPath2);

    // this is an empty file which tests the toBeRemovedFileList
    // at the end of function pruneFileListBySize
    Path emptyConfFile4 = new Path(inputPath.toUri() + "/" + "job_1399977777177_0771_conf.xml");
    os = hdfs.createNewFile(emptyConfFile4);
    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile4));
    origList[11] = hdfs.getFileStatus(emptyConfFile4);

    FileStatus [] prunedList = FileLister.pruneFileList(maxFileSize, origList);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 4);
  }

  @Test
  public void testGetJobIdFromPath() {
    String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    String jobId = FileLister.getJobIdFromPath(srcPath);
    String expJobId = "job_1329348432655_0001";
    assertEquals(expJobId, jobId);

    String JOB_CONF_FILE_NAME = "src/test/resources/job_1329348432655_0001_conf.xml";
    File jobConfFile = new File(JOB_CONF_FILE_NAME);
    srcPath = new Path(jobConfFile.toURI());
    jobId = FileLister.getJobIdFromPath(srcPath);
    assertEquals(expJobId, jobId);

    jobConfFile = new File("job_201311192236_3583_1386370578196_user1_Sleep+job");
    srcPath = new Path(jobConfFile.toURI());
    jobId = FileLister.getJobIdFromPath(srcPath);
    expJobId = "job_201311192236_3583";
    assertEquals(expJobId, jobId);

  }

  @Test(expected=ProcessingException.class)
  public void testGetJobIdFromIncorrectPath() {
    final String JOB_HISTORY_FILE_NAME = "abcd.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    FileLister.getJobIdFromPath(srcPath);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}