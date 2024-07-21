package com.microsoft.azure.kusto.data.http;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.protocol.HttpContext;

public class SimpleProxyPlanner extends DefaultRoutePlanner {
    private HttpHost proxy;
    private String[] hostPatterns;

    public SimpleProxyPlanner(String proxyHost, int proxyPort, String scheme, String nonProxyHosts) {
        super(DefaultSchemePortResolver.INSTANCE);
        proxy = new HttpHost(proxyHost, proxyPort, scheme);
        parseNonProxyHosts(nonProxyHosts);
    }

    private void parseNonProxyHosts(String nonProxyHosts) {
        if (nonProxyHosts != null) {
            String[] hosts = nonProxyHosts.split("\\|");
            hostPatterns = new String[hosts.length];
            for (int i = 0; i < hosts.length; ++i) {
                hostPatterns[i] = hosts[i].toLowerCase().replace("*", ".*?");
            }
        }
    }

    private boolean doesTargetMatchNonProxyHosts(HttpHost target) {
        if (hostPatterns == null) {
            return false;
        }
        String targetHost = target.getHostName().toLowerCase();
        for (String pattern : hostPatterns) {
            if (targetHost.matches(pattern)) return true;
        }
        return false;
    }

    @Override
    protected HttpHost determineProxy(
            final HttpHost target,
            final HttpRequest request,
            final HttpContext context) {

        return doesTargetMatchNonProxyHosts(target) ? null : proxy;
    }
}
