/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.hraven.hadoopJobMonitor.jmx;

import java.util.Date;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.twitter.hraven.hadoopJobMonitor.jmx.WhiteList;

public class WhiteListTest {
  
  String appId = "application_1392835005776_377048";

  @Before
  public void init() {
    long now = System.currentTimeMillis();
    int rnd = new Random(now).nextInt();
    //to ensure that the appId is new and not repeated in the white list stored file
    appId = "application_" + Long.toString(now) + "_" + Integer.toString(rnd);
  }
  
  @Test
  public void testIsWhiteListed() throws Exception {
    WhiteList.init("/tmp");
    WhiteList mbean = WhiteList.getInstance();
    boolean isWL = mbean.isWhiteListed(appId);
    Assert.assertFalse("The not white listsed app is reported white listted", isWL);
    
    mbean.expireAfterMinutes(appId, 10);
    isWL = mbean.isWhiteListed(appId);
    Assert.assertTrue("The white listsed app is not reported white listted", isWL);

    long now = System.currentTimeMillis();
    Date later = new Date(now + 11 * 60 * 1000);
    isWL = mbean.isWhiteListed(appId, later);
    Assert.assertFalse("The expired white listsed app is reported white listted", isWL);
  }
}
