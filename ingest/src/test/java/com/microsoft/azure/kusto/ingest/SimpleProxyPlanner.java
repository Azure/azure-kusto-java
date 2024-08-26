package com.microsoft.azure.kusto.ingest;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;

// Simple implementation of DefaultRoutePlanner that checks if the target url hostname is prefixed by given value
public class SimpleProxyPlanner extends DefaultRoutePlanner {
    private HttpHost proxy;
    private String[] nonProxyHostsPrefixes;

    public SimpleProxyPlanner(String proxyHost, int proxyPort, String scheme, String[] nonProxyHostsPrefixes) {
        super(DefaultSchemePortResolver.INSTANCE);
        proxy = new HttpHost(proxyHost, proxyPort, scheme);
        this.nonProxyHostsPrefixes = nonProxyHostsPrefixes;
    }

    @Override
    protected HttpHost determineProxy(
            final HttpHost target,
            final HttpRequest request,
            final HttpContext context) {

        if (nonProxyHostsPrefixes != null) {
            for (String nonProxyHostsPrefix : nonProxyHostsPrefixes) {
                if (target.getHostName().startsWith(nonProxyHostsPrefix)) {
                    return null;
                }
            }
        }

        return proxy;
    }
}
