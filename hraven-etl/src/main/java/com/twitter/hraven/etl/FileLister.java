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
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    Set<String> toBeRemovedFileList = new HashSet<String>();
    Set<String> alreadyMovedFileList = new HashSet<String>();
    // append the destination root dir with year/month/day
    String rootYMD = getDatedRoot(destPath.toUri().toString());
    LOG.info("Root YMD: " + rootYMD);
    Path destYMDPath = new Path(rootYMD);
    for (int i = 0; i < origList.length; i++) {
      fileSize = origList[i].getLen();

      /*
       * check if hbase can store this file if yes, consider it for processing
       */
      if (fileSize <= maxFileSize) {
        prunedFileList.add(origList[i]);
      } else {
        LOG.info("In getListFilesToProcess filesize " + fileSize + " has exceeded maxFileSize "
            + maxFileSize + " for " + origList[i].getPath().toUri());

        /*
         * if this file is huge, move both the job history and job conf files so that they are
         * retained together for processing in the future
         */
        /* the file may already have been moved as part of pair relocation */
        if(alreadyMovedFileList.contains(origList[i].getPath().getName())) {
          continue;
        }
        String fileName = relocateFiles(hdfs, origList[i].getPath(), destYMDPath);
        alreadyMovedFileList.add(origList[i].getPath().getName());
        if(StringUtils.isNotBlank(fileName)) {
          alreadyMovedFileList.add(fileName);
        }

        /*
         * the (jobConf, jobHistory) pair was relocated on hdfs but now remove it from processing
         * list Many a times, the files occur one after the other on hdfs try to see if that's the
         * case, else store the filename to be removed later
         */
        /*
         * check if the other file in (jobConf, jobHistory) pair is stored AFTER the huge file
         */
        if ((i < (origList.length - 1))
            && (fileName.equalsIgnoreCase(origList[i + 1].getPath().getName()))) {
          /*
           * skip the next record in origList since we dont want to process it at this time and we
           * already moved this file to relocation Dir
           */
          LOG.info("Skipping the next file " + origList[i + 1].getPath().toUri() + " for "
              + origList[i].getPath().toUri());
          i++;
        } else {
          /*
           * the other file could be on hdfs right before the huge file and maybe we have already
           * stored it in prunedList So then remove it
           */
          int prunedSize = prunedFileList.size();
          if ((i > 0) && (prunedSize > 0) && (fileName.equalsIgnoreCase(
                  prunedFileList.get(prunedSize-1).getPath().getName()))) {
            LOG.info("Removing from pruneList " + prunedFileList.get(prunedSize-1).getPath().toUri() + " for "
                + origList[i].getPath().toUri());
            prunedFileList.remove(prunedSize-1);
          } else {
            /*
             * Since we've looked at one file before and one file after the huge file and did not
             * find the other file in the (jobConf, jobHistory) pair, it's possibly on hdfs
             * somewhere far away from the huge file So now, store this name so that we can remove
             * it later
             */
            toBeRemovedFileList.add(fileName);
          }
        }
      }
    }
    if (prunedFileList.size() == 0) {
      LOG.info("Found no files worth processing. Returning 0 sized array");
      return new FileStatus[0];
    }
    for (int i = 0; i < prunedFileList.size(); i++) {
      if (toBeRemovedFileList.size() == 0) {
        // no files to remove
        break;
      }
      String fileName = prunedFileList.get(i).getPath().getName();
      if (toBeRemovedFileList.contains(fileName)) {
        LOG.info("Removing from prunedList " + fileName);
        prunedFileList.remove(i);
        toBeRemovedFileList.remove(fileName);
      }
    }
    return prunedFileList.toArray(new FileStatus[prunedFileList.size()]);
  }

  /**
   * creates dest root and relocates the job conf and job history files
   * @param hdfs - filesystem to be looked at
   * @param sourcePath - source path of the file to be moved
   * @param destPath - relocation destination root dir
   */
  static String relocateFiles(FileSystem hdfs, Path sourcePath, Path destPath) {

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

    /*
     * move this huge file to relocation dir
     */
    moveFileHdfs(hdfs, sourcePath, destPath);

    /*
     * now move the corresponding file in the
     * (jobConf, jobHistory) pair as well
     */
    return relocateConfPairFile(hdfs, sourcePath, destPath);

  }

  /**
   * Once the huge file  has been moved,
   * relocate the corresponding other file in
   * the (jobConf, jobHistory) pair as well
   *
   * @param hdfs - filesystem to be looked at
   * @param sourcePath - input source path
   * @param destPath - relocation dir
   */
  static String relocateConfPairFile (FileSystem hdfs, Path sourcePath, Path destPath) {
    JobFile jf = new JobFile(sourcePath.getName());
    String confFileName = "";
    if(jf.isJobHistoryFile()){
      confFileName = JobFile.getConfFileName(jf.getFilename());
      Path confPath = new Path(sourcePath.getParent().toUri() + "/" + confFileName);
      LOG.info("Relocating confFile " + confPath.toUri() + " to " + destPath.toUri());
      moveFileHdfs(hdfs, confPath, destPath);
    }
    return confFileName;
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
      if (moved != true) {
        throw new ProcessingException("Could not move from " + src
            + " to " + destFullPathStr);
      }
    } catch (IOException ioe) {
      throw new ProcessingException("Could not move from " + src
            + " to " + destFullPathStr, ioe);
    }
  }
}
