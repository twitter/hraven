package com.twitter.hraven.rest.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.hraven.util.JSONUtil;

class UrlDataLoader<T> {

    private static final Log LOG = LogFactory.getLog(UrlDataLoader.class);

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

      RequestConfig requestConfig =
          RequestConfig.custom()
              .setConnectTimeout(connectTimeout)
              .setConnectionRequestTimeout(connectTimeout)
              .setSocketTimeout(readTimeout).build();
      HttpClientBuilder httpClientBuilder =
          HttpClientBuilder.create().setDefaultRequestConfig(requestConfig);

      if (! useCompression) {
        LOG.info("Not using compression!");
        httpClientBuilder.disableContentCompression();
      } else {
        LOG.debug("Using compression by default! Trying gzip, deflate");
      }

      CloseableHttpClient httpClient = httpClientBuilder.build();
      HttpGet httpGet = new HttpGet(endpointURL);
      HttpResponse response = httpClient.execute(httpGet);

      try {
        input = response.getEntity().getContent();
        return (List<T>) JSONUtil.readJson(input, typeRef);
      } finally {
        IOUtils.closeQuietly(input);
        IOUtils.closeQuietly(httpClient);
      }
    }
}
