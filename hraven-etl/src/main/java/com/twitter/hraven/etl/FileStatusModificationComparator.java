/*
Copyright 2012 Twitter, Inc.

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

import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;

public class FileStatusModificationComparator implements Comparator<FileStatus> {

  /**
   * Default constructor.
   */
  public FileStatusModificationComparator() {
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  public int compare(FileStatus fileStatus1, FileStatus fileStatus2) {

    // Do the obligatory null checks.
    if ((fileStatus1 == null) && (fileStatus2 == null)) {
      return 0;
    }
    if (fileStatus1 == null) {
      return -1;
    }
    if (fileStatus2 == null) {
      return 1;
    }

    long modificationTime1 = fileStatus1.getModificationTime();
    long modificationTime2 = fileStatus2.getModificationTime();

    return (modificationTime1 < modificationTime2 ? -1
        : (modificationTime1 == modificationTime2 ? 0 : 1));
  };

}
