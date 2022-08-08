// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;

class E2ETest {
    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static ClientImpl queryClient;
    private static final String databaseName = System.getenv("TEST_DATABASE");
    private static final String appId = System.getenv("APP_ID");
    private static String appKey;
    private static final String tenantId = System.getenv().getOrDefault("TENANT_ID", "microsoft.com");
    private static String principalFqn;
    private static String resourcesPath;
    private static int currentCount = 0;
    private static List<TestDataItem> dataForTests;
    private static String tableName;
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";

    @BeforeAll
    public static void setUp() throws IOException {
        appKey = System.getenv("APP_KEY");
        if (appKey == null) {
            String secretPath = System.getProperty("SecretPath");
            if (secretPath == null) {
                throw new IllegalArgumentException("SecretPath is not set");
            }
            appKey = Files.readAllLines(Paths.get(secretPath)).get(0);
        }

        tableName = "JavaTest_" + new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS").format(Calendar.getInstance().getTime()) + "_"
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        principalFqn = String.format("aadapp=%s;%s", appId, tenantId);

        ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("DM_CONNECTION_STRING"), appId, appKey,
                tenantId);
        dmCsb.setUserNameForTracing("testUser");
        try {
            ingestClient = IngestClientFactory.createClient(dmCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create ingest client", ex);
        }

        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("ENGINE_CONNECTION_STRING"), appId,
                appKey, tenantId);
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(engineCsb);
            queryClient = new ClientImpl(engineCsb);
            managedStreamingIngestClient = IngestClientFactory.createManagedStreamingIngestClient(dmCsb, engineCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
        }

        createTableAndMapping();
        createTestData();
    }

    @AfterAll
    public static void tearDown() {
        try {
            queryClient.executeToJsonResult(databaseName, String.format(".drop table %s ifexists", tableName));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop table", ex);
        }
    }

    private static boolean IsManualExecution() {
        return System.getenv("CI_EXECUTION") == null || !System.getenv("CI_EXECUTION").equals("1");
    }

    private static void createTableAndMapping() {
        try {
            queryClient.executeToJsonResult(databaseName, String.format(".drop table %s ifexists", tableName));
            queryClient.executeToJsonResult(databaseName, String.format(".create table %s %s", tableName, tableColumns));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop and create new table", ex);
        }

        resourcesPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();
        try {
            String mappingAsString = new String(Files.readAllBytes(Paths.get(resourcesPath, "dataset_mapping.json")));
            queryClient.executeToJsonResult(databaseName, String.format(".create table %s ingestion json mapping '%s' '%s'",
                    tableName, mappingReference, mappingAsString));
        } catch (Exception ex) {
            Assertions.fail("Failed to create ingestion mapping", ex);
        }

        try {
            queryClient.executeToJsonResult(databaseName, ".clear database cache streamingingestion schema");
        } catch (Exception ex) {
            Assertions.fail("Failed to refresh cache", ex);
        }
    }

    private static void createTestData() {
        IngestionProperties ingestionPropertiesWithoutMapping = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithoutMapping.setFlushImmediately(true);
        ingestionPropertiesWithoutMapping.setDataFormat(DataFormat.CSV);

        IngestionProperties ingestionPropertiesWithMappingReference = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithMappingReference.setFlushImmediately(true);
        ingestionPropertiesWithMappingReference.setIngestionMapping(mappingReference, IngestionMappingKind.JSON);
        ingestionPropertiesWithMappingReference.setDataFormat(DataFormat.JSON);

        IngestionProperties ingestionPropertiesWithColumnMapping = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithColumnMapping.setFlushImmediately(true);
        ingestionPropertiesWithColumnMapping.setDataFormat(DataFormat.JSON);
        ColumnMapping first = new ColumnMapping("rownumber", "int");
        first.setPath("$.rownumber");
        ColumnMapping second = new ColumnMapping("rowguid", "string");
        second.setPath("$.rowguid");
        ColumnMapping[] columnMapping = new ColumnMapping[] {first, second};
        ingestionPropertiesWithColumnMapping.setIngestionMapping(columnMapping, IngestionMappingKind.JSON);
        ingestionPropertiesWithColumnMapping.setDataFormat(DataFormat.JSON);

        dataForTests = Arrays.asList(new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv");
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv.gz");
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.json");
                rows = 2;
                ingestionProperties = ingestionPropertiesWithMappingReference;
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.json.gz");
                rows = 2;
                ingestionProperties = ingestionPropertiesWithMappingReference;
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.json");
                rows = 2;
                ingestionProperties = ingestionPropertiesWithColumnMapping;
                testOnstreamingIngestion = false; // streaming ingestion doesn't support inline mapping
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.json.gz");
                rows = 2;
                ingestionProperties = ingestionPropertiesWithColumnMapping;
                testOnstreamingIngestion = false; // streaming ingestion doesn't support inline mapping
            }
        });
    }

    private void assertRowCount(int expectedRowsCount, boolean checkViaJson) {
        int totalLoops = 30;
        int actualRowsCount = 0;

        for (int i = 0; i < totalLoops; i++) {
            try {
                Thread.sleep(i * 100);

                if (checkViaJson) {
                    String result = queryClient.executeToJsonResult(databaseName, String.format("%s | count", tableName));
                    JSONArray jsonArray = new JSONArray(result);
                    JSONObject primaryResult = null;
                    for (Object o : jsonArray) {
                        if (o.toString() != null && o.toString().matches(".*\"TableKind\"\\s*:\\s*\"PrimaryResult\".*")) {
                            primaryResult = new JSONObject(o.toString());
                            break;
                        }
                    }
                    assertNotNull(primaryResult);
                    actualRowsCount = (Integer) ((JSONArray) ((JSONArray) primaryResult.get("Rows")).get(0)).get(0) - currentCount;
                } else {
                    KustoOperationResult result = queryClient.execute(databaseName, String.format("%s | count", tableName));
                    KustoResultSetTable mainTableResult = result.getPrimaryResults();
                    mainTableResult.next();
                    actualRowsCount = mainTableResult.getInt(0) - currentCount;
                }

            } catch (Exception ex) {
                continue;
            }

            if (actualRowsCount >= expectedRowsCount) {
                break;
            }
        }
        currentCount += actualRowsCount;
        assertEquals(expectedRowsCount, actualRowsCount);
    }

    @Test
    void testShowPrincipals() {
        boolean found = isDatabasePrincipal(queryClient);
        Assertions.assertTrue(found, "Failed to find authorized AppId in the database principals");
    }

    private boolean isDatabasePrincipal(ClientImpl localQueryClient) {
        KustoOperationResult result = null;
        boolean found = false;
        try {
            result = localQueryClient.execute(databaseName, String.format(".show database %s principals", databaseName));
            // result = localQueryClient.execute(databaseName, String.format(".show version"));
        } catch (Exception ex) {
            Assertions.fail("Failed to execute show database principals command", ex);
        }
        KustoResultSetTable mainTableResultSet = result.getPrimaryResults();
        while (mainTableResultSet.next()) {
            if (mainTableResultSet.getString("PrincipalFQN").equals(principalFqn)) {
                found = true;
            }
        }
        return found;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIngestFromFile(boolean isManaged) {
        for (TestDataItem item : dataForTests) {
            FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
            try {
                (isManaged ? managedStreamingIngestClient : ingestClient).ingestFromFile(fileSourceInfo, item.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(item.rows, false);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIngestFromStream(boolean isManaged) throws FileNotFoundException {
        for (TestDataItem item : dataForTests) {
            InputStream stream = new FileInputStream(item.file);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);
            if (item.file.getPath().endsWith(".gz")) {
                streamSourceInfo.setCompressionType(CompressionType.gz);
            }
            try {
                (isManaged ? managedStreamingIngestClient : ingestClient).ingestFromStream(streamSourceInfo, item.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(item.rows, true);
        }
    }

    @Test
    void testStreamingIngestFromFile() {
        for (TestDataItem item : dataForTests) {
            if (item.testOnstreamingIngestion) {
                FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
                try {
                    streamingIngestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows, false);
            }
        }
    }

    @Test
    void testStreamingIngestFromStream() throws FileNotFoundException {
        for (TestDataItem item : dataForTests) {
            if (item.testOnstreamingIngestion && item.ingestionProperties.getIngestionMapping() != null) {
                InputStream stream = new FileInputStream(item.file);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);
                if (item.file.getPath().endsWith(".gz")) {
                    streamSourceInfo.setCompressionType(CompressionType.gz);
                }
                try {
                    streamingIngestClient.ingestFromStream(streamSourceInfo, item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows, true);
            }
        }
    }

    private boolean canAuthenticate(ConnectionStringBuilder engineCsb) {
        try {
            IngestClientFactory.createStreamingIngestClient(engineCsb);
            ClientImpl localEngineClient = new ClientImpl(engineCsb);
            assertTrue(isDatabasePrincipal(localEngineClient));
            assertTrue(isDatabasePrincipal(localEngineClient)); // Hit cache
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
            return false;
        }
        return true;
    }

    @Test
    void testCreateWithUserPrompt() {
        Assumptions.assumeTrue(IsManualExecution());
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithUserPrompt(System.getenv("ENGINE_CONNECTION_STRING"), null,
                System.getenv("USERNAME_HINT"));
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithDeviceAuthentication() {
        Assumptions.assumeTrue(IsManualExecution());
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithDeviceCode(System.getenv("ENGINE_CONNECTION_STRING"), null);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCertificate() throws GeneralSecurityException, IOException {
        Assumptions.assumeTrue(StringUtils.isNotBlank(System.getenv("PUBLIC_X509CER_FILE_LOC")));
        Assumptions.assumeTrue(StringUtils.isNotBlank(System.getenv("PRIVATE_PKCS8_FILE_LOC")));
        X509Certificate cer = SecurityUtils.getPublicCertificate(System.getenv("PUBLIC_X509CER_FILE_LOC"));
        PrivateKey privateKey = SecurityUtils.getPrivateKey(System.getenv("PRIVATE_PKCS8_FILE_LOC"));
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCertificate(System.getenv("ENGINE_CONNECTION_STRING"), appId, cer,
                privateKey, "microsoft.onmicrosoft.com");
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCredentials() {
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("ENGINE_CONNECTION_STRING"), appId,
                appKey, tenantId);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadAccessTokenAuthentication() {
        Assumptions.assumeTrue(StringUtils.isNotBlank(System.getenv("TOKEN")));
        String token = System.getenv("TOKEN");
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(System.getenv("ENGINE_CONNECTION_STRING"), token);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadTokenProviderAuthentication() {
        Assumptions.assumeTrue(StringUtils.isNotBlank(System.getenv("TOKEN")));
        Callable<String> tokenProviderCallable = () -> System.getenv("TOKEN");
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadTokenProviderAuthentication(System.getenv("ENGINE_CONNECTION_STRING"),
                tokenProviderCallable);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCloudInfoWithCluster() throws DataServiceException {
        String clusterUrl = System.getenv("ENGINE_CONNECTION_STRING");
        CloudInfo cloudInfo = CloudInfo.retrieveCloudInfoForCluster(clusterUrl);
        assertNotSame(CloudInfo.DEFAULT_CLOUD, cloudInfo);
        assertNotNull(cloudInfo);
        assertSame(cloudInfo, CloudInfo.retrieveCloudInfoForCluster(clusterUrl));
    }

    @Test
    void testCloudInfoWith404() throws DataServiceException {
        String fakeClusterUrl = "https://www.microsoft.com/";
        assertSame(CloudInfo.DEFAULT_CLOUD, CloudInfo.retrieveCloudInfoForCluster(fakeClusterUrl));
    }

    @Test
    void testParameterizedQuery() throws DataServiceException, DataClientException {
        IngestionProperties ingestionPropertiesWithoutMapping = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithoutMapping.setFlushImmediately(true);
        ingestionPropertiesWithoutMapping.setDataFormat(DataFormat.CSV);

        TestDataItem item = new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv");
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        };

        FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
        try {
            ingestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
        } catch (Exception ex) {
            Assertions.fail(ex);
        }
        assertRowCount(item.rows, false);

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter("xdoubleParam", 2.0002);
        crp.setParameter("xboolParam", false);
        crp.setParameter("xint16Param", 2);
        crp.setParameter("xint64Param", 2L);
        crp.setParameter("xdateParam", new CslDateTimeFormat("2016-01-01T01:01:01.0000000Z").getValue()); // Or can pass LocalDateTime
        crp.setParameter("xtextParam", "Two");
        crp.setParameter("xtimeParam", new CslTimespanFormat("-00:00:02.0020002").getValue()); // Or can pass Duration

        String query = String.format(
                "declare query_parameters(xdoubleParam:real, xboolParam:bool, xint16Param:int, xint64Param:long, xdateParam:datetime, xtextParam:string, xtimeParam:time); %s | where xdouble == xdoubleParam and xbool == xboolParam and xint16 == xint16Param and xint64 == xint64Param and xdate == xdateParam and xtext == xtextParam and xtime == xtimeParam",
                tableName);
        KustoOperationResult resultObj = queryClient.execute(databaseName, query, crp);
        KustoResultSetTable mainTableResult = resultObj.getPrimaryResults();
        mainTableResult.next();
        String results = mainTableResult.getString(13);
        assertEquals("Two", results);
    }

    @Test
    void testPerformanceKustoOperationResultVsJsonVsStreamingQuery() throws DataClientException, DataServiceException, IOException {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = tableName + " | take 100000"; // Best to use a table that has many records, to mimic performance use case
        StopWatch stopWatch = new StopWatch();

        // Standard approach - API converts json to KustoOperationResult
        stopWatch.start();
        KustoOperationResult resultObj = queryClient.execute(databaseName, query, clientRequestProperties);
        stopWatch.stop();
        long timeConvertedToJavaObj = stopWatch.getTime();
        System.out.printf("Convert json to KustoOperationResult result count='%s' returned in '%s'ms%n", resultObj.getPrimaryResults().count(),
                timeConvertedToJavaObj);

        // Specialized use case - API returns raw json for performance
        stopWatch.reset();
        stopWatch.start();
        String jsonResult = queryClient.executeToJsonResult(databaseName, query, clientRequestProperties);
        stopWatch.stop();
        long timeRawJson = stopWatch.getTime();
        System.out.printf("Raw json result size='%s' returned in '%s'ms%n", jsonResult.length(), timeRawJson);

        // Depends on many transient factors, but is ~15% cheaper when there are many records
        // assertTrue(timeRawJson < timeConvertedToJavaObj);

        // Specialized use case - API streams raw json for performance
        stopWatch.reset();
        stopWatch.start();
        // The InputStream *must* be closed by the caller to prevent memory leaks
        try (InputStream is = queryClient.executeStreamingQuery(databaseName, query, clientRequestProperties);
                BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            StringBuilder streamedResult = new StringBuilder();
            char[] buffer = new char[65536];
            String streamedLine;
            long prevTime = 0;
            int count = 0;
            while ((br.read(buffer)) > -1) {
                count++;
                streamedLine = String.valueOf(buffer);
                streamedResult.append(streamedLine);
                streamedResult.append("\n");
                long currTime = stopWatch.getTime();
                System.out.printf("Read streamed segment of length '%s' in '%s'ms%n", streamedLine.length(), (currTime - prevTime));
                prevTime = currTime;
            }
            stopWatch.stop();
            long timeStreaming = stopWatch.getTime();
            System.out.printf("Streamed raw json of length '%s' in '%s'ms, using '%s' streamed segments%n", streamedResult.length(), timeStreaming, count);
            assertTrue(count > 0);
        }
    }

    @Test
    void testSameHttpClientInstance() throws DataClientException, DataServiceException, URISyntaxException, IOException {
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("ENGINE_CONNECTION_STRING"), appId,
                appKey, tenantId);
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        CloseableHttpClient httpClientSpy = Mockito.spy(httpClient);
        ClientImpl clientImpl = new ClientImpl(engineCsb, httpClientSpy);

        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = tableName + " | take 1000";

        clientImpl.execute(databaseName, query, clientRequestProperties);
        clientImpl.execute(databaseName, query, clientRequestProperties);

        Mockito.verify(httpClientSpy, atLeast(2)).execute(any());
    }
}
