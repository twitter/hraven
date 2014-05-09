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

package com.twitter.hraven;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * stores the capacity related information from a fair scheduler file
 *
 */
public class FairSchedulerCapacityDetails implements CapacityDetails {

  private static final Log LOG = LogFactory.getLog(FairSchedulerCapacityDetails.class);

  public enum FairSchedulerAtttributes {
    minResources, minMaps, minReduces ;
  }

  private Map<String, Long> minResources;
  private Map<String, Long> minMaps;
  private Map<String, Long> minReduces;

  /**
   * loads the hadoop fair scheduler config file to process the queue capacity numbers
   * @return map of queue name to capacity
   * @throws ProcessingException
   */
  public void loadDetails(String fileName) throws ProcessingException {

    this.minResources = new HashMap<String, Long>();
    this.minMaps = new HashMap<String, Long>();
    this.minReduces = new HashMap<String, Long>();

    try {
      LOG.info("Loading fair scheduler:" + fileName);
      URL url = new URL(fileName);
      URLConnection connection = url.openConnection();
      Document doc = parseXML(connection.getInputStream());
      processXmlDoc(FairSchedulerAtttributes.minResources, doc);
      processXmlDoc(FairSchedulerAtttributes.minMaps, doc);
      processXmlDoc(FairSchedulerAtttributes.minReduces, doc);
    } catch (NumberFormatException nfe) {
      LOG.error("Caught NumberFormatException: " + nfe.getMessage());
    } catch (InvalidPropertiesFormatException e) {
      LOG.error("Caught InvalidPropertiesFormatException: " + e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.error("Caught FileNotFound: " + e.toString());
    } catch (IOException e) {
      LOG.error("Caught IOE " + e.toString());
    }
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Long> kv : minResources
          .entrySet()) {
        LOG.debug(" key value" + kv.getKey() + ": minResources=" + kv.getValue()
            + ": minMaps=" + minMaps.get(kv.getKey()) + ": minReduces="
            + minReduces.get(kv.getKey()));
      }
    }
  }

  /**
   * processes the xml document for different attributes
   * namely minResources, minMaps and minReduces
   * @param attribute
   * @param capacityInfo
   * @param doc
   * @return
   */
  public void processXmlDoc(
    FairSchedulerAtttributes attribute, Document doc) {
    String key = "";
    Node aNode = null;
    long value = 0L;
    NodeList descNodes = doc.getElementsByTagName(attribute.toString());
    for (int i = 0; i < descNodes.getLength(); i++) {
      aNode = descNodes.item(i);
      key = getKey(aNode);
      if (StringUtils.isBlank(key)) {
        LOG.error("Error in processing xml for " + attribute.toString() + " of "
            + aNode.getNodeName());
        continue;
      }
      value = getValue(aNode);
      switch (attribute) {
      case minResources:
        this.minResources.put(key, value);
        break;
      case minMaps:
        this.minMaps.put(key, value);
        break;
      case minReduces:
        this.minReduces.put(key, value);
        break;
      default:
        LOG.error("unknown attribute in fair scheduler : " + attribute);
        break;
      }
    }
  }

  /**
   * gets the text representation of the queue/pool name as per the XML node definition
   * @param XML Node found in fair scheduler
   * @return queue/pool name
   */
  static String getKey(Node aNode) {
    String key = null;
    Node parent = aNode.getParentNode();
    if (parent != null) {
      NamedNodeMap attributes = parent.getAttributes();
      if (attributes != null) {
        Node n = attributes.getNamedItem("name");
        if (n != null) {
          key = n.getTextContent();
        }
      }
    }
    return key;
  }

  /**
   * gets the value of the pool/queue capacity as a long
   * @param XML node
   * @return value as long
   */
  private static long getValue(Node aNode) {
    long value = 0L;
    try {
      value = Long.parseLong(aNode.getTextContent());
    } catch (NumberFormatException nfe) {
      LOG.error(" caught " + nfe.getMessage() + " for " + aNode.getNodeName());
    }
    return value;
  }

  /**
   * parses the input stream as an XML document
   * @param inputstream
   * @return xml document
   * @throws ProcessingException
   */
  public static Document parseXML(InputStream stream) throws ProcessingException {
    DocumentBuilderFactory objDocumentBuilderFactory = null;
    DocumentBuilder objDocumentBuilder = null;
    Document doc = null;
    try {
      objDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
      objDocumentBuilder = objDocumentBuilderFactory.newDocumentBuilder();
      doc = objDocumentBuilder.parse(stream);
    } catch (Exception ex) {
      LOG.error(" Exception during document loading: " + ex.getMessage());
      throw new ProcessingException(ex.getMessage());
    }
    return doc;
  }

  @Override
  public int size() {
    return Math.max(Math.max(minResources.size(), minMaps.size()),
      minReduces.size());
  }

  @Override
  public Object getAttribute(String queue, String attribute) {

    try {
      switch (FairSchedulerAtttributes.valueOf(attribute)) {
      case minResources:
        return this.minResources.get(queue);
      case minMaps:
        return this.minMaps.get(queue);
      case minReduces:
        return this.minReduces.get(queue);
      default:
        return 0L;
      }
    } catch (NullPointerException nfe) {
      LOG.error("Attribute is null, returning 0");
      return 0L;
    } catch (IllegalArgumentException iae) {
      LOG.error("No such attribute " + attribute + " " + iae);
      return 0L;
    }

  }

  @Override
  public SchedulerTypes getSchedulerType() {
    return SchedulerTypes.FAIR_SCHEDULER;
  }

}
