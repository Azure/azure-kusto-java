package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Regression tests for the v7.0.6 "double grace period" bug.
 * <p>
 * In v7.0.6, {@code ClientImpl.determineTimeout} added a 30-second client grace period to
 * its return value. However, {@code BaseClient.getContextTimeout} already adds the same
 * 30-second buffer ({@code EXTRA_TIMEOUT_FOR_CLIENT_SIDE}) when constructing the HTTP
 * request context. The result was that every request used an effective client-side
 * timeout of {@code serverTimeout + 60s} instead of {@code serverTimeout + 30s}.
 * <p>
 * The fix removes the grace from {@code determineTimeout}; this test pins
 * {@code determineTimeout} to return the raw timeout (no grace) so the regression cannot
 * be reintroduced silently.
 */
public class ClientImplTimeoutTest {

    private static final long QUERY_DEFAULT_MS = TimeUnit.MINUTES.toMillis(4);
    private static final long ADMIN_DEFAULT_MS = TimeUnit.MINUTES.toMillis(10);
    private static final long STREAMING_INGEST_DEFAULT_MS = TimeUnit.MINUTES.toMillis(10);

    private ClientImpl newClient() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        return (ClientImpl) ClientFactory.createClient(csb);
    }

    @Test
    @DisplayName("determineTimeout returns default query timeout (no grace added at this layer)")
    void defaultQueryTimeout() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        long actual = client.determineTimeout(crp, CommandType.QUERY, "https://testcluster.kusto.windows.net");
        Assertions.assertEquals(QUERY_DEFAULT_MS, actual);
    }

    @Test
    @DisplayName("determineTimeout returns default admin command timeout (no grace added at this layer)")
    void defaultAdminTimeout() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        long actual = client.determineTimeout(crp, CommandType.ADMIN_COMMAND, "https://testcluster.kusto.windows.net");
        Assertions.assertEquals(ADMIN_DEFAULT_MS, actual);
    }

    @Test
    @DisplayName("determineTimeout returns default streaming ingest timeout (no grace added at this layer)")
    void defaultStreamingIngestTimeout() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        long actual = client.determineTimeout(crp, CommandType.STREAMING_INGEST, "https://testcluster.kusto.windows.net");
        Assertions.assertEquals(STREAMING_INGEST_DEFAULT_MS, actual);
    }

    @Test
    @DisplayName("determineTimeout returns user-provided timeout verbatim (no grace added at this layer)")
    void userProvidedTimeout() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        long userTimeout = TimeUnit.MINUTES.toMillis(7);
        crp.setTimeoutInMilliSec(userTimeout);

        long actual = client.determineTimeout(crp, CommandType.QUERY, "https://testcluster.kusto.windows.net");
        Assertions.assertEquals(userTimeout, actual);
    }

    @Test
    @DisplayName("determineTimeout returns Long.MAX_VALUE when OPTION_NO_REQUEST_TIMEOUT is true")
    void noRequestTimeoutOption() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setOption(ClientRequestProperties.OPTION_NO_REQUEST_TIMEOUT, true);

        long actual = client.determineTimeout(crp, CommandType.QUERY, "https://testcluster.kusto.windows.net");
        Assertions.assertEquals(Long.MAX_VALUE, actual);
    }

    @Test
    @DisplayName("determineTimeout writes the raw (no-grace) timeout back into the server-timeout option")
    void serverTimeoutHeaderUnaffectedByGrace() throws URISyntaxException {
        ClientImpl client = newClient();
        ClientRequestProperties crp = new ClientRequestProperties();
        client.determineTimeout(crp, CommandType.QUERY, "https://testcluster.kusto.windows.net");

        // Server-timeout option must reflect the raw default (4 minutes), not a graced value.
        Assertions.assertEquals("00:04:00", crp.getOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT));
    }
}
