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
import com.twitter.hraven.JobId;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.util.ByteUtil;

/**
 */
public class JobKeyConverter implements ByteConverter<JobKey> {
  private JobIdConverter idConv = new JobIdConverter();

  /**
   * Returns the byte encoded representation of a JobKey
   *
   * @param jobKey the JobKey to serialize
   * @return the byte encoded representation of the JobKey
   */
  @Override
  public byte[] toBytes(JobKey jobKey) {
    if (jobKey == null) {
      return Constants.EMPTY_BYTES;
    } else {
      return ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes(jobKey.getCluster()),
          Bytes.toBytes(jobKey.getUserName()),
          Bytes.toBytes(jobKey.getAppId()),
          Bytes.toBytes(jobKey.getEncodedRunId()),
          idConv.toBytes(jobKey.getJobId()));
    }
  }

  /**
   * Reverse operation of
   * {@link JobKeyConverter#toBytes(com.twitter.hraven.JobKey)}
   *
   * @param bytes the serialized version of a JobKey
   * @return a deserialized JobKey instance
   */
  @Override
  public JobKey fromBytes(byte[] bytes) {
    byte[][] splits = splitJobKey(bytes);
    // no combined runId + jobId, parse as is
    return parseJobKey(splits);
  }

  /**
   * Constructs a JobKey instance from the individual byte encoded key
   * components.
   *
   * @param keyComponents
   *          as split on
   * @return a JobKey instance containing the decoded components
   */
  public JobKey parseJobKey(byte[][] keyComponents) {
    // runId is inverted in the bytes representation so we get reverse
    // chronological order
    long encodedRunId = keyComponents.length > 3 ?
        Bytes.toLong(keyComponents[3]) : Long.MAX_VALUE;

    JobId jobId = keyComponents.length > 4 ?
        idConv.fromBytes(keyComponents[4]) : null;

    return new JobKey(Bytes.toString(keyComponents[0]),
        (keyComponents.length > 1 ? Bytes.toString(keyComponents[1]) : null),
        (keyComponents.length > 2 ? Bytes.toString(keyComponents[2]) : null),
        Long.MAX_VALUE - encodedRunId,
        jobId);
  }

  /**
   * Handles splitting the encoded job key correctly, accounting for long
   * encoding of the run ID.  Since the long encoding of the run ID may
   * legitimately contain the separator bytes, we first split the leading 3
   * elements (cluster!user!appId), then split out the runId and remaining
   * fields based on the encoded long length;
   *
   * @param rawKey byte encoded representation of the job key
   * @return
   */
  static byte[][] splitJobKey(byte[] rawKey) {
    byte[][] splits = ByteUtil.split(rawKey, Constants.SEP_BYTES, 4);

    /* final components (runId!jobId!additional) need to be split separately for correct
     * handling of runId long encoding */
    if (splits.length == 4) {
      // TODO: this splitting is getting really ugly, look at using Orderly instead for keying
      byte[] remainder = splits[3];
      byte[][] extraComponents = new byte[3][];

      // now extract components (runId!jobId!additional)
      // run id occurs at the start and is of 8 bytes in length
      extraComponents[0] = extractRunId(remainder, Constants.RUN_ID_LENGTH_JOBKEY);
      // job id occurs after run id and seperator
      int offset = Constants.RUN_ID_LENGTH_JOBKEY + Constants.SEP_BYTES.length;
      extraComponents[1] = extractJobId(offset, remainder);

      // now extract any additional stuff after the job id
      if (extraComponents[1] != null) {
        offset += extraComponents[1].length;
      }
      offset += Constants.SEP_BYTES.length;
      extraComponents[2] = extractRemainder(offset, remainder);

      int extraSize = 0;
      // figure out the full size of all splits
      for (int i = 0; i < extraComponents.length; i++) {
        if (extraComponents[i] != null) {
          extraSize++;
        } else {
          break; // first null signals hitting the end of remainder
        }
      }

      byte[][] allComponents = new byte[3+extraSize][];
      // fill in the first 3 elts
      for (int i=0; i < 3; i++) {
        allComponents[i] = splits[i];
      }
      // add any extra that were non-null
      for (int i=0; i < extraSize; i++) {
        allComponents[3+i] = extraComponents[i];
      }

      return allComponents;
    }
    return splits;
  }

  private static byte[] extractRemainder(int offset, byte[] remainder) {
    return ByteUtil.safeCopy(remainder, offset,
      remainder.length - offset);
  }

  private static int getLengthJobIdPackedBytes(int offset, byte[] remainder) {
    int lengthRest = remainder.length - offset ;
    byte[] jobIdOtherStuff = null;
    if (lengthRest > offset) {
      jobIdOtherStuff = ByteUtil.safeCopy(remainder, offset,
        remainder.length-offset);
    } else {
      jobIdOtherStuff = new byte[0];
    }
    byte[][] splitRunIdJobIdExtra = ByteUtil.split(jobIdOtherStuff,
        Constants.SEP_BYTES);
    int lengthJobId = (splitRunIdJobIdExtra.length >= 1 ?
        splitRunIdJobIdExtra[0].length : 0);
    return lengthJobId;
  }
  /**
   * extracts the job id from the packged byte array
   * array looks like encodedRunid!jobid!otherstuff
   * @param remainder
   * @return
   */
  private static byte[] extractJobId(int offset, byte[] remainder) {
    // since remainder contains runid ! jobid ! possibly other stuff
    // the offset for reading job is:
    //          8 bytes for run id
    //           +
    //          bytes for separator field
    int lengthJobId = getLengthJobIdPackedBytes(offset, remainder);

//    if (remainder.length >= (offset + length_jobId)) {
      return ByteUtil.safeCopy(remainder, offset, lengthJobId);
//    } else {
//      return Constants.EMPTY_BYTES;
//    }
  }

  /**
   * extracts a long number representation of encoded run id
   * it reads 8 bytes
   * @param remainder
   */
  private static byte[] extractRunId(byte[] remainder, int lengthToRead) {
//    if (remainder.length >= lengthToRead) {
      return ByteUtil.safeCopy(remainder, 0, lengthToRead);
//    } else {
//      return Constants.EMPTY_BYTES;
//    }
  }
}
