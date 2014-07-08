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
package com.twitter.hraven.util;

import com.twitter.hraven.Constants;

/**
 * Utility class for string manipulation.
 */
public class StringUtil {

  private static final String SPACE = " ";
  private static final String UNDERSCORE = "_";

  /**
   * Takes a string token to be used as a key or qualifier and cleanses out reserved tokens. This
   * operation is not symetrical. Logic is to replace all spaces and exclamation points with
   * underscores.
   *
   * @param token token to cleanse.
   * @return
   */
  public static String cleanseToken(String token) {
    if (token == null || token.length() == 0) { return token; };

    token = token.trim();
    String cleansed = token.replaceAll(SPACE, UNDERSCORE);
    cleansed = cleansed.replaceAll(Constants.SEP, UNDERSCORE);

    return cleansed;
  }
}
