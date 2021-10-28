package com.microsoft.azure.kusto.data;

public class HttpClientProperties {

  private final Integer maxIdleTime;
  private final Boolean keepAlive;
  private final Integer maxKeepAlive;
  private final Integer maxConnectionTotal;
  private final Integer maxConnectionRoute;

  private HttpClientProperties(Builder builder) {
    this.maxIdleTime = builder.maxIdleTime;
    this.keepAlive = builder.keepAlive;
    this.maxKeepAlive = builder.maxKeepAlive;
    this.maxConnectionTotal = builder.maxConnectionTotal;
    this.maxConnectionRoute = builder.maxConnectionRoute;
  }

  public Builder build() {
    return new Builder();
  }

  public Integer maxIdleTime() {
    return maxIdleTime;
  }

  public Boolean isKeepAlive() {
    return keepAlive;
  }

  public Integer maxKeepAlive() {
    return maxKeepAlive;
  }

  public Integer maxConnectionTotal() {
    return maxConnectionTotal;
  }

  public Integer maxConnectionRoute() {
    return maxConnectionRoute;
  }

  static class Builder {
    private Integer maxIdleTime;
    private Boolean keepAlive;
    private Integer maxKeepAlive;
    private Integer maxConnectionTotal;
    private Integer maxConnectionRoute;

    private Builder() { }

    public Builder maxIdleTime(Integer maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      return this;
    }

    public Builder keepAlive(Boolean keepAlive) {
      this.keepAlive = keepAlive;
      return this;
    }

    public Builder maxKeepAlive(Integer maxKeepAlive) {
      this.maxKeepAlive = maxKeepAlive;
      return this;
    }

    public Builder maxConnectionTotal(Integer maxConnectionTotal) {
      this.maxConnectionTotal = maxConnectionTotal;
      return this;
    }

    public Builder maxConnectionRoute(Integer maxConnectionRoute) {
      this.maxConnectionRoute = maxConnectionRoute;
      return this;
    }

    public HttpClientProperties build() {
      return new HttpClientProperties(this);
    }
  }

}
