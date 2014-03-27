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
import java.util.ArrayList;
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
   * @return - array of FileStatus of files to be processed
   * @throws IOException
   */
  public static FileStatus[] getListFilesToProcess(long maxFileSize, boolean recurse,
      FileSystem hdfs, Path inputPath, JobFileModifiedRangePathFilter pathFilter)
      throws IOException {

    LOG.info(" in getListFilesToProcess maxFileSize=" + maxFileSize
        + " inputPath= " + inputPath.toUri());
    FileStatus[] origList = listFiles(recurse, hdfs, inputPath, pathFilter);
    if (origList == null) {
      LOG.info(" No files found, orig list returning 0");
      return new FileStatus[0];
    }
    return pruneFileListBySize(maxFileSize, origList, hdfs, inputPath);
   }

  /**
   * prunes the given list/array of files based on their sizes
   *
   * @param maxFileSize -max #bytes to be stored in an hbase cell
   * @param origList - input list of files to be processed
   * @param hdfs - filesystem to be looked at
   * @param inputPath - root dir of the path containing history files
   * @return - pruned array of FileStatus of files to be processed
   */
  static FileStatus[] pruneFileListBySize(long maxFileSize, FileStatus[] origList, FileSystem hdfs,
      Path inputPath) {
    LOG.info("Pruning orig list  of size " + origList.length + " for source" + inputPath.toUri());

    long fileSize = 0L;
    List<FileStatus> prunedFileList = new ArrayList<FileStatus>();

    Set<String> toBeRemovedJobId = new HashSet<String>();
    // append the destination root dir with year/month/day
    for (int i = 0; i < origList.length; i++) {
      fileSize = origList[i].getLen();

      // check if hbase can store this file if yes, consider it for processing
      if (fileSize <= maxFileSize) {
        prunedFileList.add(origList[i]);
      } else {
        Path hugeFile = origList[i].getPath();
        LOG.info("In getListFilesToProcess filesize " + fileSize + " has exceeded maxFileSize "
            + maxFileSize + " for " + hugeFile.toUri());

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
        LOG.info("Removing from prunedList " + curFile.toUri());
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
}
