package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class HeaderTest {

    @Test
    public void testHeadersDefault() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        Map<String, String> headers = client.extractTracingHeaders(new ClientRequestProperties());
        Assertions.assertNotNull(headers.get("x-ms-app"));
        Assertions.assertNotNull(headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
    }

    @Test
    public void testHeadersWithCustomCsb() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.setClientVersionForTracing("testVersion");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        Map<String, String> headers = client.extractTracingHeaders(new ClientRequestProperties());
        Assertions.assertEquals("testApp", headers.get("x-ms-app"));
        Assertions.assertEquals("testUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
        Assertions.assertTrue(headers.get("x-ms-client-version").endsWith("|testVersion"));
    }

    @Test
    public void testHeadersWithCustomCsbAndClientRequestProperties() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.setClientVersionForTracing("testVersion");
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setApplication("crpApp");
        crp.setUser("crpUser");

        Map<String, String> headers = client.extractTracingHeaders(crp);
        Assertions.assertEquals("crpApp", headers.get("x-ms-app"));
        Assertions.assertEquals("crpUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
        Assertions.assertTrue(headers.get("x-ms-client-version").endsWith("|testVersion"));
    }

    @Test
    public void testSetConnectorNameAndVersion() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", false, null, null, null);
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        ClientRequestProperties crp = new ClientRequestProperties();

        Map<String, String> headers = client.extractTracingHeaders(crp);
        Assertions.assertEquals("[none]", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertTrue(headers.get("x-ms-app").startsWith("Kusto.myConnector:{myVersion}"));
    }

    @Test
    public void testSetConnectorNoAppVersion() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", true, "myApp", null, null);
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        ClientRequestProperties crp = new ClientRequestProperties();

        Map<String, String> headers = client.extractTracingHeaders(crp);
        Assertions.assertTrue(headers.get("x-ms-user").length() > 0);
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertTrue(headers.get("x-ms-app").startsWith("Kusto.myConnector:{myVersion}"));
    }

    @Test
    public void testSetConnectorFull() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", true, "myUser", "myApp", "myAppVersion", Pair.of("myField", "myValue"));
        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);
        ClientRequestProperties crp = new ClientRequestProperties();

        Map<String, String> headers = client.extractTracingHeaders(crp);
        Assertions.assertEquals("myUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertEquals("Kusto.myConnector:{myVersion}|App.{myApp}:{myAppVersion}|myField:{myValue}", headers.get("x-ms-app"));
    }

    @Test
    public void testWew() throws URISyntaxException, DataServiceException, DataClientException, InterruptedException {
        Client client = ClientFactory.createClient(ConnectionStringBuilder.createWithUserPrompt("https://sdkse2etest.eastus.kusto.windows.net"));
        KustoOperationResult data = client.execute("fastbatchinge2e",
                "datatable (a:int) [1000]| extend x = tostring(range(1.0, 100000, 1))| extend y = todynamic(range(1.0, 100000, 1))");
        KustoResultSetTable primaryResults = data.getPrimaryResults();

        for (int rowNum = 1; primaryResults.next(); rowNum++) {
            KustoResultColumn[] columns = primaryResults.getColumns();
            List<Object> currentRow = primaryResults.getCurrentRow();
            System.out.printf("Record %s%n", rowNum);

            for (int j = 0; j < currentRow.size(); j++) {
                Object cell = currentRow.get(j);
                System.out.printf("Column: '%s' of type '%s', Value: '%s'%n", columns[j].getColumnName(), columns[j].getColumnType(),
                        cell == null ? "[null]" : cell);
            }

            System.out.println();
        }

        Thread.sleep(100000);

    }
}
