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
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class that performs operations on hdfs files such as listing files recursively
 * Used by JobFilePartitioner and JobFilePreprocessor
 *
 */
public class FileLister {

  /**
   * Default constructor.
   */
  public FileLister() {
  }

  /*
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

  /*
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
}
