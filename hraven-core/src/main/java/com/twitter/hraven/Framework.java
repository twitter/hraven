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
package com.twitter.hraven;

/**
 * Used to distinguish the framework used to launch the map-reduce job with.
 */
public enum Framework {

  /**
   * Identifies Pig applications/ pig scripts
   */
  PIG("p", "pig"),
  /**
   * 
   */
  SCALDING("s", "scalding"),
  /**
   * 
   */
  NONE("n", "none, plain map-reduce");

  // TODO: Add Hive as a framework and at least recognize those jobs as such.

  /**
   * The code representing this application type as used in the {@link JobDesc}
   */
  private final String code;

  /**
   * The description for this {@link Framework}
   */
  private final String description;

  /**
   * Constructor
   * 
   * @param code
   *          for this type
   * @param description
   *          for this type
   */
  private Framework(String code, String description) {
    this.code = code;
    this.description = description;
  }

  /**
   * @return the code corresponding to this type.
   */
  public String getCode() {
    return code;
  }

  /**
   * @return the description for this type.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Get the {@link Framework} corresponding to this code, or none if not
   * specifically Pig or Scalding
   * 
   * @param code
   */
  public static Framework get(String code) {

    for (Framework framework : Framework.values()) {
      if (framework.getCode().equals(code)) {
        return framework;
      }
    }
    return NONE;
  }

}
