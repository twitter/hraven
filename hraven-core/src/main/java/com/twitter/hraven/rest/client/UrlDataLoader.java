package com.twitter.hraven.rest.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.hraven.util.JSONUtil;

class UrlDataLoader<T> {

    private static final Log LOG = LogFactory.getLog(UrlDataLoader.class);

    private String endpointURL;
    private TypeReference typeRef;
    private int connectTimeout;
    private int readTimeout;

    /**
     * Constructor.
     * @param endpointUrl
     * @param t TypeReference for json deserialization, should be TypeReference<List<T>>.
     * @throws java.io.IOException
     */
    public UrlDataLoader(String endpointUrl, TypeReference t, int connectTimeout, int readTimeout)
        throws IOException {
      this.endpointURL = endpointUrl;
      this.typeRef = t;
      this.connectTimeout = connectTimeout;
      this.readTimeout = readTimeout;
    }

    @SuppressWarnings("unchecked")
    public List<T> load() throws IOException {
      InputStream input = null;
      try {
        URL url = new URL(endpointURL);
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        input = connection.getInputStream();
        return (List<T>) JSONUtil.readJson(input, typeRef);
      } finally {
        if (input != null) {
          try {
            input.close();
          } catch (IOException e) {
            LOG.warn(e);
          }
        }
      }
    }
}

