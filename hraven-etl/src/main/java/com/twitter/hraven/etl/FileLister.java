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
package com.twitter.hraven.etl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Utility class that performs operations on hdfs files such as listing files recursively
 * Used by JobFilePartitioner and JobFilePreprocessor
 *
 */
public class FileLister {

  private static final Log LOG = LogFactory.getLog(FileLister.class);

  /**
   * Default constructor.
   */
  public FileLister() {
  }

  /**
   * Recursively traverses the dirs to get the list of
   * files for a given path filtered as per the input path range filter
   *
   */
  private static void traverseDirs(List<FileStatus> fileStatusesList, FileSystem hdfs,
      Path inputPath, JobFileModifiedRangePathFilter jobFileModifiedRangePathFilter)
          throws IOException
  {
    // get all the files and dirs in the current dir
    FileStatus allFiles[] = hdfs.listStatus(inputPath);
    for (FileStatus aFile: allFiles) {
      if (aFile.isDir()) {
        //recurse here
        traverseDirs(fileStatusesList, hdfs, aFile.getPath(), jobFileModifiedRangePathFilter);
      }
      else {
        // check if the pathFilter is accepted for this file
        if (jobFileModifiedRangePathFilter.accept(aFile.getPath())) {
          fileStatusesList.add(aFile);
        }
      }
    }
  }

  /**
   * Gets the list of files for a given path filtered as per the input path range filter
   * Can go into directories recursively
   *
   * @param recurse - whether or not to traverse recursively
   * @param hdfs - the file system
   * @param inputPath - the path to traverse for getting the list of files
   * @param jobFileModifiedRangePathFilter - the filter to include/exclude certain files
   *
   * @return array of file status.
   * @throws IOException
   */
  public static FileStatus[] listFiles (boolean recurse, FileSystem hdfs, Path inputPath,
      JobFileModifiedRangePathFilter jobFileModifiedRangePathFilter) throws IOException
  {
    if (recurse) {
      List<FileStatus> fileStatusesList = new ArrayList<FileStatus>();
      traverseDirs(fileStatusesList, hdfs, inputPath, jobFileModifiedRangePathFilter);
      FileStatus[] fileStatuses = (FileStatus[]) fileStatusesList.toArray(
          new FileStatus[fileStatusesList.size()]);
      return fileStatuses;
    }
    else {
      return hdfs.listStatus(inputPath, jobFileModifiedRangePathFilter);
    }
  }

  /**
   * looks at the src path and fetches the list of files to process
   * confirms that the size of files
   * is less than the maxFileSize
   * hbase cell can't store files bigger than maxFileSize,
   * hence no need to consider them for rawloading
   * Reference: {@link https://github.com/twitter/hraven/issues/59}
   * @param maxFileSize - max #bytes to be stored in an hbase cell
   * @param recurse - whether to recurse or not
   * @param hdfs - filesystem to be looked at
   * @param inputPath - root dir of the path containing history files
   * @param jobFileModifiedRangePathFilter - to filter out files
   * @param destPath - relocation dir for huge files
   * @return - array of FileStatus of files to be processed
   * @throws IOException
   */
  public static FileStatus[] getListFilesToProcess(long maxFileSize, boolean recurse,
      FileSystem hdfs, Path inputPath, JobFileModifiedRangePathFilter pathFilter,
      Path destPath)
      throws IOException {

    LOG.info(" in getListFilesToProcess maxFileSize=" + maxFileSize
        + " inputPath= " + inputPath.toUri()
        + " destPath=" + destPath.toUri());
    FileStatus[] origList = listFiles(recurse, hdfs, inputPath, pathFilter);
    if (origList == null) {
      LOG.info(" No files found, orig list returning 0");
      return new FileStatus[0];
    }
    return pruneFileListBySize(maxFileSize, origList, hdfs, inputPath, destPath);
   }

  /**
   * prunes the given list/array of files based on their sizes
   *
   * @param maxFileSize -max #bytes to be stored in an hbase cell
   * @param origList - input list of files to be processed
   * @param hdfs - filesystem to be looked at
   * @param inputPath - root dir of the path containing history files
   * @param destPath - relocation dir for huge files
   * @return - pruned array of FileStatus of files to be processed
   */
  static FileStatus[] pruneFileListBySize(long maxFileSize, FileStatus[] origList, FileSystem hdfs,
      Path inputPath, Path destPath) {
    LOG.info("Pruning orig list  of size " + origList.length + " for source" + inputPath.toUri()
        + " to move to " + destPath.toUri());

    long fileSize = 0L;
    List<FileStatus> prunedFileList = new ArrayList<FileStatus>();

    Set<String> toBeRemovedJobId = new HashSet<String>();
    // append the destination root dir with year/month/day
    String rootYMD = getDatedRoot(destPath.toUri().toString());
    LOG.info("Root YMD: " + rootYMD);
    Path destYMDPath = new Path(rootYMD);
    for (int i = 0; i < origList.length; i++) {
      fileSize = origList[i].getLen();

      // check if hbase can store this file if yes, consider it for processing
      if (fileSize <= maxFileSize) {
        prunedFileList.add(origList[i]);
      } else {
        Path hugeFile = origList[i].getPath();
        LOG.info("In getListFilesToProcess filesize " + fileSize + " has exceeded maxFileSize "
            + maxFileSize + " for " + hugeFile.toUri());

         // if this file is huge, relocate it
        relocateFile(hdfs, hugeFile, destYMDPath);
        // note the job id so that we can remove the other file (job conf or job history)
        toBeRemovedJobId.add(getJobIdFromPath(hugeFile));
      }
    }
    if (prunedFileList.size() == 0) {
      LOG.info("Found no files worth processing. Returning 0 sized array");
      return new FileStatus[0];
    }

    String jobId = null;
    ListIterator<FileStatus> it = prunedFileList.listIterator();
    while (it.hasNext()) {
      if (toBeRemovedJobId.size() == 0) {
        // no files to remove
        break;
      }
      Path curFile = it.next().getPath();
      jobId = getJobIdFromPath(curFile);
      if (toBeRemovedJobId.contains(jobId)) {
        LOG.info("Relocating and removing from prunedList " + curFile.toUri());
        relocateFile(hdfs, curFile, destYMDPath);
        it.remove();
        /*
         * removing the job id from the hash set since there would be only
         * one file with this job id in the prunedList, the other file with
         * this job id was huge and was already moved out
         */
        toBeRemovedJobId.remove(jobId);
      }
    }
    return prunedFileList.toArray(new FileStatus[prunedFileList.size()]);
  }

  /**
   * extracts the job id from a Path
   * @param input Path
   * @return job id as string
   */
  static String getJobIdFromPath(Path aPath) {
    String fileName = aPath.getName();
    JobFile jf = new JobFile(fileName);
    String jobId = jf.getJobid();
    if(jobId == null) {
      throw new ProcessingException("job id is null for " + aPath.toUri());
    }
    return jobId;
  }

  /**
   * creates dest root and relocates the job conf and job history files
   * @param hdfs - filesystem to be looked at
   * @param sourcePath - source path of the file to be moved
   * @param destPath - relocation destination root dir
   */
  static void relocateFile(FileSystem hdfs, Path sourcePath, Path destPath) {

    try {
      // create it on hdfs if it does not exist
      if (!hdfs.exists(destPath)) {
        LOG.info("Attempting to create " + destPath);
        boolean dirCreated = hdfs.mkdirs(destPath);
        if (!dirCreated) {
          throw new ProcessingException("Could not create " + destPath.toUri());
        }
      }
    } catch (IOException ioe) {
      throw new ProcessingException("Could not check/create " + destPath.toUri(), ioe);
    }

    // move this huge file to relocation dir
    moveFileHdfs(hdfs, sourcePath, destPath);

  }

  /**
   * constructs a string for
   * givenPath/year/month/day
   *
   * @return path/year/month/day
   */
  static String getDatedRoot(String root) {

    if(root == null) {
      return null;
    }
    DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
    String formatted = df.format(new Date());
    return root + "/" + formatted ;
  }

  /**
   * moves a file on hdfs from sourcePath to a subdir under destPath
   */
  public static void moveFileHdfs(FileSystem hdfs, Path sourcePath, Path destPath) {
    if (destPath == null || sourcePath == null) {
      LOG.error("source or destination path is null, not moving ");
      return;
    }

    String src = sourcePath.toUri().toString();
    // construct the final destination path including file name
    String destFullPathStr = destPath.toUri() + "/" + sourcePath.getName();
    Path destFullPath = new Path(destFullPathStr);

    try {
      LOG.info("Moving " + sourcePath.toUri() + " to " + destFullPathStr);
      // move the file on hdfs from source to destination
      boolean moved = hdfs.rename(sourcePath, destFullPath);
      if (!moved) {
        throw new ProcessingException("Could not move from " + src
            + " to " + destFullPathStr);
      }
    } catch (IOException ioe) {
      throw new ProcessingException("Could not move from " + src
            + " to " + destFullPathStr, ioe);
    }
  }
}
