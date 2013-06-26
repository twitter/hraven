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
package com.twitter.hraven.datasource;

import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.TaskKey;

/**
 */
public class TaskKeyConverter implements ByteConverter<TaskKey> {
  private JobKeyConverter jobKeyConv = new JobKeyConverter();

  /**
   * Returns the bytes representation for a TaskKey.
   *
   * @param taskKey
   *          the TaskKey instance to serialize
   * @return the serialized representation of the TaskKey
   */
  @Override
  public byte[] toBytes(TaskKey taskKey) {
    return Bytes.add(jobKeyConv.toBytes(taskKey), Constants.SEP_BYTES,
        Bytes.toBytes(taskKey.getTaskId()));
  }

  /**
   * Generates a TaskKey from the byte encoded format.
   *
   * @param bytes the serialized version of a task key
   * @return the deserialized TaskKey instance
   */
  @Override
  public TaskKey fromBytes(byte[] bytes) {
    byte[][] keyComponents = JobKeyConverter.splitJobKey(bytes);
    JobKey jobKey = jobKeyConv.parseJobKey(keyComponents);
    return new TaskKey(jobKey,
        (keyComponents.length > 5 ? Bytes.toString(keyComponents[5]) : null));
  }
}
