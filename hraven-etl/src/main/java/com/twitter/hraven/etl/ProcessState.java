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

/**
 * Keeps track of the state of the processing of a bunch of job conf and job
 * history files.
 * 
 */
public enum ProcessState {

  /**
   * When processing has just started, but no complete set of job files has been
   * moved to the processing directory yet.
   */
  CREATED(0),

  /**
   * Pre-processing step is complete. The number of processed files will
   * indicate how many files have been processed.
   */
  PREPROCESSED(1),

  /**
   * The loading step is complete. The number of processed files will indicate
   * how many files have been processed. The record will now also have a min and
   * a max job ID processed.
   */
  LOADED(2),

  /**
   * All job files between the min and the max job ID for a given cluster are
   * processed.
   */
  PROCESSED(3);

  /**
   * Representing this state.
   */
  private final int code;

  private ProcessState(int code) {
    this.code = code;
  }

  /**
   * @return the code for this state
   */
  public int getCode() {
    return code;
  }

  /**
   * @param code
   *          representing the state
   * @return the ProcessState for this code, or if not recognized, then return
   *         {@link ProcessState#CREATED}
   */
  public static ProcessState getProcessState(int code) {
    for (ProcessState state : ProcessState.values()) {
      if (state.getCode() == code) {
        return state;
      }
    }
    return CREATED;
  }

}
