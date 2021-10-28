package com.microsoft.azure.kusto.data;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SingletonHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int MAX_CONN_TOTAL = 40;
  private static final int MAX_CONN_ROUTE = 40;
  private static final int MAX_IDLE_TIME = 120;
  private static SingletonHttpClient INSTANCE;

  private final CloseableHttpClient httpClient;
  private final Optional<HttpClientProperties> properties;

  private SingletonHttpClient(HttpClientProperties properties) {
    this.properties = Optional.ofNullable(properties);
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
      .useSystemProperties()
      .setMaxConnTotal(getOrElse(HttpClientProperties::maxConnectionTotal, MAX_CONN_TOTAL))
      .setMaxConnPerRoute(getOrElse(HttpClientProperties::maxConnectionRoute, MAX_CONN_ROUTE))
      .evictExpiredConnections()
      .evictIdleConnections(getOrElse(HttpClientProperties::maxIdleTime, MAX_IDLE_TIME), TimeUnit.SECONDS);

    if (getOrElse(HttpClientProperties::isKeepAlive, true)) {
      final ConnectionKeepAliveStrategy keepAliveStrategy = this.properties
        .map(HttpClientProperties::maxKeepAlive)
        .map(CustomConnectionKeepAliveStrategy::new)
        .orElseGet(CustomConnectionKeepAliveStrategy::new);

      this.httpClient = httpClientBuilder
        .setKeepAliveStrategy(keepAliveStrategy)
        .build();
    } else {
      this.httpClient = httpClientBuilder.build();
    }
  }

  private <T> T getOrElse(Function<HttpClientProperties, T> getter, T defaultValue) {
    return properties.map(getter).orElse(defaultValue);
  }

  public static SingletonHttpClient of(HttpClientProperties properties) {
    if (INSTANCE == null) {
      INSTANCE = new SingletonHttpClient(properties);
    }
    return INSTANCE;
  }

  public CloseableHttpClient getHttpClient() {
    return httpClient;
  }

  public void close() {
    try {
      LOGGER.info("Closing http client");
      INSTANCE.httpClient.close();
    } catch (IOException e) {
      LOGGER.warn("Couldn't close HttpClient.");
    }
  }

  static class CustomConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

    private final int defaultKeepAlive;

    CustomConnectionKeepAliveStrategy() {
      this(2 * 60); // default to 2 min
    }

    CustomConnectionKeepAliveStrategy(int defaultKeepAlive) {
      this.defaultKeepAlive = defaultKeepAlive;
    }

    @Override
    public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
      // honor 'keep-alive' header
      HeaderElementIterator it = new BasicHeaderElementIterator
        (httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE));
      while (it.hasNext()) {
        HeaderElement he = it.nextElement();
        String param = he.getName();
        String value = he.getValue();
        if (value != null && param.equalsIgnoreCase
          ("timeout")) {
          return Long.parseLong(value) * 1000;
        }
      }
      // otherwise keep alive for default seconds
      return defaultKeepAlive * 1000L;
    }
  }

}
