package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Map;

public class HeaderTest {

    @Test
    public void testHeadersDefault() throws URISyntaxException, DataServiceException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        Map<String, String> headers = client.extractTracingHeaders(new ClientRequestProperties());
        Assertions.assertEquals("com.intellij.rt.junit.JUnitStarter", headers.get("x-ms-app"));
        Assertions.assertNotNull(headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
    }

    @Test
    public void testHeadersWithCustomCsb() throws URISyntaxException, DataServiceException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.appendClientVersionForTracing("testVersion");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        Map<String, String> headers = client.extractTracingHeaders(new ClientRequestProperties());
        Assertions.assertEquals("testApp", headers.get("x-ms-app"));
        Assertions.assertEquals("testUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
        Assertions.assertTrue(headers.get("x-ms-client-version").endsWith("[testVersion]"));
    }

    @Test
    public void testHeadersWithCustomCsbAndClientRequestProperties() throws URISyntaxException, DataServiceException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.appendClientVersionForTracing("testVersion");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setApplication("crpApp");
        crp.setUser("crpUser");
        crp.setVersion("crpVersion");

        Map<String, String> headers = client.extractTracingHeaders(crp);
        Assertions.assertEquals("crpApp", headers.get("x-ms-app"));
        Assertions.assertEquals("crpUser", headers.get("x-ms-user"));
        Assertions.assertEquals("crpVersion", headers.get("x-ms-client-version"));
    }
}
