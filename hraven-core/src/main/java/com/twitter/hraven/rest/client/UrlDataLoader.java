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

    private static final String ACCEPT_ENCODING = "Accept-Encoding";

    private String endpointURL;
    private TypeReference typeRef;
    private int connectTimeout;
    private int readTimeout;
    private boolean useCompression;

    /**
     * Constructor, defaults to using compression (gzip / deflate).
     * @param endpointUrl
     * @param t TypeReference for json deserialization, should be TypeReference<List<T>>.
     * @throws java.io.IOException
     */
    public UrlDataLoader(String endpointUrl, TypeReference t, int connectTimeout, int readTimeout)
        throws IOException {
      this(endpointUrl, t, connectTimeout, readTimeout, true);
    }

    /**
     * Constructor.
     * @param endpointUrl
     * @param t TypeReference for json deserialization, should be TypeReference<List<T>>.
     * @throws java.io.IOException
    */
    public UrlDataLoader(String endpointUrl, TypeReference t, int connectTimeout, int readTimeout,
                         boolean useCompression) throws IOException {
      this.endpointURL = endpointUrl;
      this.typeRef = t;
      this.connectTimeout = connectTimeout;
      this.readTimeout = readTimeout;
      this.useCompression = useCompression;
    }

    @SuppressWarnings("unchecked")
    public List<T> load() throws IOException {
      InputStream input = null;
      try {
        URL url = new URL(endpointURL);
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        if (useCompression) {
          connection.setRequestProperty(ACCEPT_ENCODING, "gzip, deflate");
        }
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

