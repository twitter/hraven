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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WhiteList implements WhiteListMBean {
  public static final Log LOG = LogFactory.getLog(WhiteList.class);
  static final String WHITELIST_FILENAME = "vulture.whitelist.txt";
  private Map<String, Date> expirationMap = new TreeMap<String, Date>();
  private String whiteListDir;
  private volatile static WhiteList INSTANCE;

  public static WhiteList getInstance() {
    return INSTANCE;
  }

  public static void init(String whiteListDir) throws IOException {
    WhiteList wl = new WhiteList(whiteListDir);
    wl.load();
    LOG.error("Initial whitelist is: " + wl.getExpirations());
    INSTANCE = wl;
  }

  public synchronized String load() throws IOException {
    String filePath = whiteListDir + "/" + WHITELIST_FILENAME;
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(filePath));
      String line = reader.readLine();
      while (line != null) {
        String[] words = line.split(" ");
        if (words.length < 2)
          throw new IOException("Malformatted line: " + line);
        String appId = words[0];
        long expMs = Long.parseLong(words[1]);
        Date expDate = new Date(expMs);
        expirationMap.put(appId, expDate);
        line = reader.readLine();
      }
    } catch (FileNotFoundException e) {
      LOG.error("The white list init file does not exist (ok if it is the first time): " + filePath);
      return "No file to load form: " + filePath;
    } finally {
      if (reader != null)
        reader.close();
    }
    referesh();
    LOG.warn("WhiteList loaded from " + filePath);
    return "Load from " + filePath;
  }

  public synchronized String store() throws IOException {
    referesh();
    String filePath = whiteListDir + "/" + WHITELIST_FILENAME;
    String newFilePath = filePath + ".new";
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(newFilePath, "UTF-8");
      Iterator<Map.Entry<String, Date>> iterator =
          expirationMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Date> entry = iterator.next();
        Date date = entry.getValue();
        writer.println(entry.getKey() + " " + date.getTime() + " " + date);
      }
    } finally {
      if (writer != null)
        writer.close();
    }
    // copy the new file to the standard place
    File file = new File(filePath);
    File newFile = new File(newFilePath);
    file.delete();
    FileUtils.moveFile(newFile, file);
    LOG.warn("WhiteList stored to " + filePath);
    return "Stored in " + filePath;
  }

  private WhiteList(String whiteListDir) {
    this.whiteListDir = whiteListDir;
  }

  @Override
  public synchronized Map<String, Date> getExpirations() {
    referesh();
    return new TreeMap<String, Date>(expirationMap);
  }

  synchronized void referesh() {
    Date now = new Date();
    Iterator<Map.Entry<String, Date>> iterator =
        expirationMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Date> entry = iterator.next();
      if (entry.getValue().before(now)) {
        LOG.warn("Removing stale entry from the white list: " + entry);
        iterator.remove();
      }
    }
  }

  @Override
  public synchronized Date expireAfterMinutes(String appId, int minutes)
      throws Exception {
    long now = System.currentTimeMillis();
    if (minutes < 0)
      throw new Exception("minutes cannot be negative: " + minutes);
    long afterMs = minutes * 60 * 1000;
    long future = now + afterMs;
    Date expDate = new Date(future);
    LOG.error("WhiteList update: app=" + appId + " expirs after " + minutes
        + " minutes => expirationDate: " + expDate);
    expirationMap.put(appId, expDate);
    store();
    return expDate;
  }

  public static void main(String[] args) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName("com.twitter.vulture.jmx:type=WhiteList");
    WhiteList.init("/tmp");
    WhiteList mbean = WhiteList.getInstance();
    mbs.registerMBean(mbean, name);
    System.out.println("Waiting forever...");
    Thread.sleep(Long.MAX_VALUE);
  }

  public boolean isWhiteListed(String appId) {
    Date now = new Date();
    return isWhiteListed(appId, now);
  }
  
  public boolean isWhiteListed(String appId, Date now) {
    Date expDate = expirationMap.get(appId);
    boolean whiteListed = expDate != null && expDate.after(now);
    if (expDate != null && ! whiteListed)
      expirationMap.remove(appId);
    return whiteListed;
  }
}
