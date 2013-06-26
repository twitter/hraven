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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * {@link PathFilter} that accepts only job conf or job history files.
 *
 */
public class JobFilePathFilter implements PathFilter {

  /**
   * Default constructor.
   */
  public JobFilePathFilter() {
  }
  
  /*
   * Accept only those paths that are either job confs or job history files.
   * 
   * @see org.apache.hadoop.fs.PathFilter#accept(org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean accept(Path path) {
    // Ideally we want to do this
    // JobFile jobFile = new JobFile(path.getName());
    // return (jobFile.isJobConfFile() || jobFile.isJobHistoryFile());
    // Aside from that not being efficient, it also chokes on input directories.
    
    // therefore, allow anythying but CRC files. The record reader will have to deal with the rest.
    return !((path == null) || (path.getName().endsWith(".crc")));
  }

}
