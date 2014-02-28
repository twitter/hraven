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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileLister {
  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
  }

  @Test
  public void testMoveFileHdfs() throws IOException {
    Path src = new Path("/dir1/file1234.txt");
    Path dest = new Path("/dir3/dir00004");
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    boolean os = hdfs.createNewFile(src);
    assertTrue(os);
    os = hdfs.mkdirs(dest);
    assertTrue(os);
    FileLister.moveFileHdfs(hdfs, src, dest);
    String destFullPathStr = dest.toUri() + "/" + src.getName();
    Path expFile = new Path(destFullPathStr);
    assertTrue(hdfs.exists(expFile));
    assertFalse(hdfs.exists(src));
  }

  @Test
  public void testMoveFileHdfsNullDest() throws IOException {
    Path src = new Path("/dir1/dir12345");
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    boolean os = hdfs.createNewFile(src);
    assertTrue(os);
    FileLister.moveFileHdfs(hdfs, src, null);
    assertTrue(hdfs.exists(src));
  }

  @Test
  public void testGetDatedRootNull() {
    assertNull(FileLister.getDatedRoot(null));
  }

  @Test
  public void testGetDatedRoot() {
    String root = "abc";
    Calendar now = Calendar.getInstance();
    StringBuilder rootYMD = new StringBuilder();
    rootYMD.append(root);
    rootYMD.append("/");
    rootYMD.append(Integer.toString(now.get(Calendar.YEAR)));
    rootYMD.append("/");
    // month is 0 based
    rootYMD.append(Integer.toString(now.get(Calendar.MONTH) + 1));
    rootYMD.append("/");
    rootYMD.append(Integer.toString(now.get(Calendar.DAY_OF_MONTH)));
    rootYMD.append("/");

    String actualRoot = FileLister.getDatedRoot(root);
    assertNotNull(actualRoot);
    assertEquals(actualRoot, rootYMD.toString());
  }

  @Test
  public void testPruneFileListBySize() throws IOException {

    Long maxFileSize = 20L;
    FileStatus[] origList = new FileStatus[2];
    FileSystem hdfs = FileSystem.get(UTIL.getConfiguration());
    Path destPath = new Path("/inputdir_filesize");
    boolean os = hdfs.mkdirs(destPath);
    assertTrue(os);
    assertTrue(hdfs.exists(destPath));

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    Path srcPath = new Path(jobHistoryfile.toURI());
    System.out.println(" src path " + srcPath.toUri());
    hdfs.copyFromLocalFile(srcPath, destPath);
    Path expPath = new Path(destPath.toUri() + "/" + srcPath.getName());
    System.out.println(" exp path " + expPath.toUri());
    assertTrue(hdfs.exists(expPath));
    origList[0] = hdfs.getFileStatus(expPath);
    System.out.println(" orig list " + origList[0].getPath().getName());

    final String JOB_CONF_FILE_NAME =
        "src/test/resources/job_1329348432655_0001_conf.xml";
    File jobConfFile = new File(JOB_CONF_FILE_NAME);
    srcPath = new Path(jobConfFile.toURI());
    System.out.println(" src path " + srcPath.toUri());
    hdfs.copyFromLocalFile(srcPath, destPath);
    expPath = new Path(destPath.toUri() + "/" + srcPath.getName());
    System.out.println(" exp path " + expPath.toUri());
    assertTrue(hdfs.exists(expPath));
    origList[1] = hdfs.getFileStatus(expPath);
    System.out.println(" orig list " + origList[1].getPath().getName());

    Path relocationPath = new Path("/relocation_filesize");
    os = hdfs.mkdirs(destPath);
    assertTrue(os);
    assertTrue(hdfs.exists(destPath));

    FileStatus [] prunedList = FileLister.pruneFileListBySize(maxFileSize, origList, hdfs, destPath, relocationPath);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 0);

    Path emptyFile = new Path(destPath.toUri() + "/" + "job_1329341111111_0101-1329111113227-user2-Sleep.jhist");
    os = hdfs.createNewFile(emptyFile);
    System.out.println(" creating... " + emptyFile.toUri());
    assertTrue(os);
    assertTrue(hdfs.exists(emptyFile));
    origList[0] = hdfs.getFileStatus(emptyFile);

    Path emptyConfFile = new Path(destPath.toUri() + "/" + "job_1329341111111_0101_conf.xml");
    os = hdfs.createNewFile(emptyConfFile);

    System.out.println(" creating... " + emptyConfFile.toUri());
    assertTrue(os);
    assertTrue(hdfs.exists(emptyConfFile));
    origList[1] = hdfs.getFileStatus(emptyConfFile);

    prunedList = FileLister.pruneFileListBySize(maxFileSize, origList, hdfs, destPath, relocationPath);
    assertNotNull(prunedList);
    assertTrue(prunedList.length == 2);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}