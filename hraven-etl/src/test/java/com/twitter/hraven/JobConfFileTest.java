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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.twitter.hraven.Constants;

public class JobConfFileTest {

  @Test
  public void test() {
    
    Pattern pigLogfilePattern = Pattern
        .compile(Constants.PIG_LOGFILE_PATTERN_REGEX);
    
    // /var/log/pig/pig_1334818693838.log
    String pigLogfile = "/var/log/pig/pig_1334818693838.log";
    Matcher matcher = pigLogfilePattern.matcher(pigLogfile);
    
    String runId = null;
    if (matcher.matches()) {
      runId = matcher.group(1);
      // TODO: validate this parsing (testcase?!?)
    } else {
      runId = "blah";
    }
    
    String appId = "Distributed Lzo Indexer [/tmp/akamai/akamai";
    appId = "[/tmp/akamai/akamai";
    
    int firstOpenBracketPos = appId.indexOf("[");
    if (firstOpenBracketPos > -1) {
      appId = appId.substring(0, firstOpenBracketPos).trim();
    }
    System.out.println("appId="+appId+".");
    
  }

}
