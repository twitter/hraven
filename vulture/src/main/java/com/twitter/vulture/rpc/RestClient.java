package com.twitter.vulture.rpc;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * The client to facilitates getting xml over remote restful api
 */
public class RestClient {
  static final Log LOG = LogFactory.getLog(RestClient.class);
  private DefaultHttpClient httpClient = new DefaultHttpClient();
  private static DocumentBuilderFactory docBldrFactory = DocumentBuilderFactory
      .newInstance();

  public static final ThreadLocal<DocumentBuilder> docBuilder =
      new ThreadLocal<DocumentBuilder>() {
        @Override
        protected DocumentBuilder initialValue() {
          try {
            return docBldrFactory.newDocumentBuilder();
          } catch (ParserConfigurationException e) {
            LOG.fatal("Error in parsing XML", e);
            return null;
          }
        }
      };

  private final static RestClient SINGLTON = new RestClient();

  /**
   * @return the singlton instance
   */
  public static RestClient getInstance() {
    return SINGLTON;
  }

  public void shutdown() {
    httpClient.getConnectionManager().shutdown();
  }

  /**
   * get an xml from a remote url
   * 
   * @param url
   * @return Document representing xml
   * @throws RestException
   */
  public Document getXml(String url) throws RestException {
    HttpGet getRequest = new HttpGet(url);
    getRequest.addHeader("accept", "application/xml");
    try {
      HttpResponse response = httpClient.execute(getRequest);
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException("Failed : HTTP error code : "
            + response.getStatusLine().getStatusCode());
      }
      InputStream xmlStream = response.getEntity().getContent();

      docBuilder.get().reset();
      Document xmlDoc;
      xmlDoc = docBuilder.get().parse(xmlStream);
      // document contains the complete XML as a Tree.
      return xmlDoc;
    } catch (Exception e) {
      throw new RestException(url, e);
    }
  }

  public static class RestException extends Exception {
    private static final long serialVersionUID = -6586571654467117663L;

    public RestException(String xmlUrl, Exception orig) {
      super("Error in getting xml at " + xmlUrl, orig);
    }
  }

  /**
   * Used for testing the RestClient
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Document xmlDoc =
        RestClient.getInstance().getXml(
            "http://hadoop-tst-rm.atla.twitter.com:8080/"
                + "ws/v1/history/mapreduce/jobs/job_1389724922546_0058/");
    // Iterating through the nodes and extracting the data.
    NodeList nodeList = xmlDoc.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      // We have encountered an <employee> tag.
      Node node = nodeList.item(i);
      if (node instanceof Element) {
        System.out.println(node.getNodeName() + " = " + node.getTextContent());
      }
    }
  }
}
