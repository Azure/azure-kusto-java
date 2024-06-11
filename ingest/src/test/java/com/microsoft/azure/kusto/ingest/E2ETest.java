// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.auth.endpoints.MatchRule;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.SecurityUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import org.apache.http.conn.util.InetAddressUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static com.microsoft.azure.kusto.ingest.IngestClientBase.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;

class E2ETest {

    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static Client queryClient;
    private static Client dmCslClient;
    private static StreamingClient streamingClient;

    // IN ORDER TO RUN THESE E2E TESTS YOU NEED THESE ENVIRONMENT VARIABLES
    private static final String DB_NAME = System.getenv("TEST_DATABASE");
    private static final String APP_ID = System.getenv("APP_ID");
    private static final String APP_KEY = System.getenv("APP_KEY");
    private static final String TENANT_ID = System.getenv().getOrDefault("TENANT_ID", "microsoft.com");
    private static final String DM_CONN_STR = System.getenv("DM_CONNECTION_STRING");
    private static final String ENG_CONN_STR = System.getenv("ENGINE_CONNECTION_STRING");
    private static final String UNAME_HINT = System.getenv("USERNAME_HINT");
    private static final String TOKEN = System.getenv("TOKEN");
    private static final String PUBLIC_X509CER_FILE_LOC = System.getenv("PUBLIC_X509CER_FILE_LOC");
    private static final String PRIVATE_PKCS8_FILE_LOC = System.getenv("PRIVATE_PKCS8_FILE_LOC");

    private static String principalFqn;
    private static String resourcesPath;
    private static int currentCount = 0;
    private static List<TestDataItem> dataForTests;
    private static String tableName;
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";
    private final ObjectMapper objectMapper = Utils.getObjectMapper();

    @BeforeAll
    public static void setUp() {
        tableName = "JavaTest_" + new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS").format(Calendar.getInstance().getTime()) + "_"
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        principalFqn = String.format("aadapp=%s;%s", APP_ID, TENANT_ID);

        ConnectionStringBuilder dmCsb = createConnection(DM_CONN_STR);
        dmCsb.setUserNameForTracing("testUser");
        try {
            dmCslClient = ClientFactory.createClient(dmCsb);
            ingestClient = IngestClientFactory.createClient(dmCsb, HttpClientProperties.builder()
                    .keepAlive(true)
                    .maxKeepAliveTime(120)
                    .maxIdleTime(60)
                    .maxConnectionsPerRoute(50)
                    .maxConnectionsTotal(50)
                    .build());
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create ingest client", ex);
        }

        ConnectionStringBuilder engineCsb = createConnection(ENG_CONN_STR);
        engineCsb.setUserNameForTracing("Java_E2ETest_ø");
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(engineCsb);
            queryClient = ClientFactory.createClient(engineCsb);
            streamingClient = ClientFactory.createStreamingClient(engineCsb);
            managedStreamingIngestClient = IngestClientFactory.createManagedStreamingIngestClient(dmCsb, engineCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
        }

        createTableAndMapping();
        createTestData();
    }

    private static @NotNull ConnectionStringBuilder createConnection(String connectionString) {
        if (APP_KEY == null || APP_KEY.isEmpty()) {
            return ConnectionStringBuilder.createWithAzureCli(connectionString);
        }
        return ConnectionStringBuilder.createWithAadApplicationCredentials(connectionString, APP_ID, APP_KEY, TENANT_ID);
    }

    @AfterAll
    public static void tearDown() {
        try {
            queryClient.executeToJsonResult(DB_NAME, String.format(".drop table %s ifexists", tableName));
            ingestClient.close();
            managedStreamingIngestClient.close();
        } catch (Exception ex) {
            Assertions.fail("Failed to drop table", ex);
        }
    }

    private static boolean isManualExecution() {
        return false;
    }

    private static void createTableAndMapping() {
        try {
            queryClient.executeToJsonResult(DB_NAME, String.format(".drop table %s ifexists", tableName));
            queryClient.executeToJsonResult(DB_NAME, String.format(".create table %s %s", tableName, tableColumns));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop and create new table", ex);
        }

        resourcesPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();
        try {
            String mappingAsString = new String(Files.readAllBytes(Paths.get(resourcesPath, "dataset_mapping.json")));
            queryClient.executeToJsonResult(DB_NAME, String.format(".create table %s ingestion json mapping '%s' '%s'",
                    tableName, mappingReference, mappingAsString));
        } catch (Exception ex) {
            Assertions.fail("Failed to create ingestion mapping", ex);
        }

        try {
            queryClient.executeToJsonResult(DB_NAME, ".clear database cache streamingingestion schema");
        } catch (Exception ex) {
            Assertions.fail("Failed to refresh cache", ex);
        }
    }

    private static void createTestData() {
        IngestionProperties ingestionPropertiesWithoutMapping = new IngestionProperties(DB_NAME, tableName);
        ingestionPropertiesWithoutMapping.setFlushImmediately(true);
        ingestionPropertiesWithoutMapping.setDataFormat(DataFormat.CSV);

        IngestionProperties ingestionPropertiesWithIgnoreFirstRecord = new IngestionProperties(DB_NAME, tableName);
        ingestionPropertiesWithIgnoreFirstRecord.setFlushImmediately(true);
        ingestionPropertiesWithIgnoreFirstRecord.setDataFormat(DataFormat.CSV);
        ingestionPropertiesWithIgnoreFirstRecord.setIgnoreFirstRecord(true);

        IngestionProperties ingestionPropertiesWithMappingReference = new IngestionProperties(DB_NAME, tableName);
        ingestionPropertiesWithMappingReference.setFlushImmediately(true);
        ingestionPropertiesWithMappingReference.setIngestionMapping(mappingReference, IngestionMappingKind.JSON);
        ingestionPropertiesWithMappingReference.setDataFormat(DataFormat.JSON);

        IngestionProperties ingestionPropertiesWithColumnMapping = new IngestionProperties(DB_NAME, tableName);
        ingestionPropertiesWithColumnMapping.setFlushImmediately(true);
        ingestionPropertiesWithColumnMapping.setDataFormat(DataFormat.JSON);
        ColumnMapping first = new ColumnMapping("rownumber", "int");
        first.setPath("$.rownumber");
        ColumnMapping second = new ColumnMapping("rowguid", "string");
        second.setPath("$.rowguid");
        ColumnMapping[] columnMapping = new ColumnMapping[] {first, second};
        ingestionPropertiesWithColumnMapping.setIngestionMapping(columnMapping, IngestionMappingKind.JSON);
        ingestionPropertiesWithColumnMapping.setDataFormat(DataFormat.JSON);

        IngestionProperties ingestionPropertiesWithTableReportMethod = new IngestionProperties(DB_NAME, tableName);
        ingestionPropertiesWithTableReportMethod.setFlushImmediately(true);
        ingestionPropertiesWithTableReportMethod.setDataFormat(DataFormat.CSV);
        ingestionPropertiesWithTableReportMethod.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);

        dataForTests = Arrays.asList(new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv");
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv");
                rows = 9;
                ingestionProperties = ingestionPropertiesWithIgnoreFirstRecord;
                testOnstreamingIngestion = false;
                testOnManaged = false;
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
        }, new TestDataItem() {
            {
                file = new File(resourcesPath, "dataset.csv");
                rows = 10;
                ingestionProperties = ingestionPropertiesWithTableReportMethod;
                testOnstreamingIngestion = false;
                testOnManaged = false;
                testReportMethodTable = true;
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
                    String result = queryClient.executeToJsonResult(DB_NAME, String.format("%s | count", tableName));
                    JsonNode jsonNode = objectMapper.readTree(result);
                    ArrayNode jsonArray = jsonNode.isArray() ? (ArrayNode) jsonNode : null;
                    JsonNode primaryResult = null;
                    Assertions.assertNotNull(jsonArray, "JsonArray cant be null since we need to retrieve primary result values out of it. ");
                    for (JsonNode o : jsonArray) {
                        if (o != null && o.toString().matches(".*\"TableKind\"\\s*:\\s*\"PrimaryResult\".*")) {
                            primaryResult = objectMapper.readTree(o.toString());
                            break;
                        }
                    }
                    Assertions.assertNotNull(primaryResult, "Primary result cant be null since we need the row count");
                    actualRowsCount = (primaryResult.get("Rows")).get(0).get(0).asInt() - currentCount;
                } else {
                    KustoOperationResult result = queryClient.executeQuery(DB_NAME, String.format("%s | count", tableName));
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

//    @Test
//    void testShowPrincipalsAsync() {
//        Mono<KustoOperationResult> laterResult = queryClient.executeMgmtAsync(DB_NAME, String.format(".show database %s principals", DB_NAME), null);
//        StepVerifier.create(laterResult)
//                .expectNextCount(1L)
//                .expectNextMatches(this::resultContainsPrincipal)
//                .expectComplete()
//                .verify();
//    }

    private boolean isDatabasePrincipal(Client localQueryClient) {
        KustoOperationResult result = null;
        try {
            result = localQueryClient.executeMgmt(DB_NAME, String.format(".show database %s principals", DB_NAME));
            // result = localQueryClient.execute(databaseName, String.format(".show version"));
        } catch (Exception ex) {
            Assertions.fail("Failed to execute show database principals command", ex);
        }
        return resultContainsPrincipal(result);
    }

    private boolean resultContainsPrincipal(KustoOperationResult result) {
        boolean found = false;
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
            if (!item.testReportMethodTable) {
                FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
                try {
                    ((isManaged && item.testOnManaged) ? managedStreamingIngestClient : ingestClient).ingestFromFile(fileSourceInfo, item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows, false);
            }
        }
    }

    @Test
    void testIngestFromFileWithTable() {
        for (TestDataItem item : dataForTests) {
            if (item.testReportMethodTable) {
                FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
                try {
                    ingestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows, false);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIngestFromStream(boolean isManaged) throws IOException {
        for (TestDataItem item : dataForTests) {
            if (item.file.getPath().endsWith(".gz")) {
                InputStream stream = Files.newInputStream(item.file.toPath());
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);

                streamSourceInfo.setCompressionType(CompressionType.gz);
                try {
                    ((isManaged && item.testOnManaged) ? managedStreamingIngestClient : ingestClient).ingestFromStream(streamSourceInfo,
                            item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows, true);
            }
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
    void testStreamingIngestFromStream() throws IOException {
        for (TestDataItem item : dataForTests) {
            if (item.testOnstreamingIngestion && item.ingestionProperties.getIngestionMapping() != null) {
                InputStream stream = Files.newInputStream(item.file.toPath());
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
            Client localEngineClient = ClientFactory.createClient(engineCsb);
            assertTrue(isDatabasePrincipal(localEngineClient));
            assertTrue(isDatabasePrincipal(localEngineClient)); // Hit cache
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
            return false;
        }
        return true;
    }

    private void assertUrlCompare(String connectionDataSource, String clusterUrl, boolean autoCorrectEndpoint, boolean isQueued) {
        if (!autoCorrectEndpoint || clusterUrl.contains(INGEST_PREFIX) || isReservedHostname(clusterUrl)) {
            String host = clusterUrl.replaceFirst("https" + PROTOCOL_SUFFIX, "");
            if (InetAddressUtils.isIPv6Address(host)) {
                String compareString = clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + '[') + ']';
                assertEquals(compareString.toLowerCase(), connectionDataSource);
            } else {
                assertEquals(clusterUrl, connectionDataSource);
            }
        } else {
            String compareString = isQueued ? clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX) : clusterUrl;
            assertEquals(compareString, connectionDataSource);
        }
    }

    @Test
    void testCreateWithUserPrompt() {
        Assumptions.assumeTrue(isManualExecution());
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithUserPrompt(ENG_CONN_STR, null, UNAME_HINT);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithConnectionStringAndUserPrompt() {
        Assumptions.assumeTrue(isManualExecution());
        ConnectionStringBuilder engineCsb = new ConnectionStringBuilder("Data Source=" + ENG_CONN_STR + ";User ID=" + UNAME_HINT);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithDeviceAuthentication() {
        Assumptions.assumeTrue(isManualExecution());
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithDeviceCode(ENG_CONN_STR, null);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCertificate() throws GeneralSecurityException, IOException {
        Assumptions.assumeTrue(StringUtils.isNotBlank(PUBLIC_X509CER_FILE_LOC));
        Assumptions.assumeTrue(StringUtils.isNotBlank(PRIVATE_PKCS8_FILE_LOC));
        X509Certificate cer = SecurityUtils.getPublicCertificate(PUBLIC_X509CER_FILE_LOC);
        PrivateKey privateKey = SecurityUtils.getPrivateKey(PRIVATE_PKCS8_FILE_LOC);
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCertificate(
                ENG_CONN_STR, APP_ID, cer, privateKey, "microsoft.onmicrosoft.com");
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCredentials() {
        Assumptions.assumeTrue(APP_KEY != null);
        ConnectionStringBuilder engineCsb = createConnection(ENG_CONN_STR);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithConnectionStringAndAadApplicationCredentials() {
        Assumptions.assumeTrue(APP_KEY != null);
        ConnectionStringBuilder engineCsb = new ConnectionStringBuilder(
                "Data Source=" + ENG_CONN_STR + ";AppClientId=" + APP_ID + ";AppKey=" + APP_KEY + ";Authority ID=" + TENANT_ID);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadAccessTokenAuthentication() {
        Assumptions.assumeTrue(StringUtils.isNotBlank(TOKEN));
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(ENG_CONN_STR, TOKEN);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithAadTokenProviderAuthentication() {
        Assumptions.assumeTrue(StringUtils.isNotBlank(TOKEN));
        Callable<String> tokenProviderCallable = () -> TOKEN;
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadTokenProviderAuthentication(ENG_CONN_STR, tokenProviderCallable);
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCloudInfoWithCluster() throws DataServiceException {
        String clusterUrl = ENG_CONN_STR;
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
        IngestionProperties ingestionPropertiesWithoutMapping = new IngestionProperties(DB_NAME, tableName);
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
        KustoOperationResult resultObj = queryClient.executeQuery(DB_NAME, query, crp);
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
        KustoOperationResult resultObj = queryClient.executeQuery(DB_NAME, query, clientRequestProperties);
        stopWatch.stop();
        long timeConvertedToJavaObj = stopWatch.getTime();
        System.out.printf("Convert json to KustoOperationResult result count='%s' returned in '%s'ms%n", resultObj.getPrimaryResults().count(),
                timeConvertedToJavaObj);

        // Specialized use case - API returns raw json for performance
        stopWatch.reset();
        stopWatch.start();
        String jsonResult = queryClient.executeToJsonResult(DB_NAME, query, clientRequestProperties);
        stopWatch.stop();
        long timeRawJson = stopWatch.getTime();
        System.out.printf("Raw json result size='%s' returned in '%s'ms%n", jsonResult.length(), timeRawJson);

        // Depends on many transient factors, but is ~15% cheaper when there are many records
        // assertTrue(timeRawJson < timeConvertedToJavaObj);

        // Specialized use case - API streams raw json for performance
        stopWatch.reset();
        stopWatch.start();
        // The InputStream *must* be closed by the caller to prevent memory leaks
        try (InputStream is = streamingClient.executeStreamingQuery(DB_NAME, query, clientRequestProperties);
             BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            StringBuilder streamedResult = new StringBuilder();
            char[] buffer = new char[65536];
            String streamedLine;
            long prevTime = 0;
            int count = 0;
            int read;
            while ((read = br.read(buffer)) > -1) {
                count++;
                streamedLine = String.valueOf(buffer, 0, read);
                streamedResult.append(streamedLine);
                streamedResult.append("\n");
                long currTime = stopWatch.getTime();
                System.out.printf("Read streamed segment of length '%s' in '%s'ms%n", read, (currTime - prevTime));
                prevTime = currTime;
            }
            stopWatch.stop();
            long timeStreaming = stopWatch.getTime();
            System.out.printf("Streamed raw json of length '%s' in '%s'ms, using '%s' streamed segments%n", streamedResult.length(), timeStreaming, count);
            assertTrue(count > 0);
        }
    }

    @Test
    void testSameHttpClientInstance() throws DataClientException, DataServiceException, URISyntaxException {
        ConnectionStringBuilder engineCsb = createConnection(ENG_CONN_STR);
        HttpClient httpClient = HttpClientFactory.create(null);
        HttpClient httpClientSpy = Mockito.spy(httpClient);
        Client clientImpl = ClientFactory.createClient(engineCsb, httpClientSpy);

        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = tableName + " | take 1000";

        clientImpl.executeQuery(DB_NAME, query, clientRequestProperties);
        clientImpl.executeQuery(DB_NAME, query, clientRequestProperties);

        // Todo potentially need a try with resources here
        Mockito.verify(httpClientSpy, atLeast(2)).sendSync(any(), eq(Context.NONE));
    }

    @Test
    void testNoRedirectsCloudFail() {
        KustoTrustedEndpoints.addTrustedHosts(Collections.singletonList(new MatchRule("statusreturner.azurewebsites.net", false)), false);
        List<Integer> redirectCodes = Arrays.asList(301, 302, 307, 308);
        redirectCodes.parallelStream().map(code -> {
            try {
                Client client = ClientFactory.createClient(
                        ConnectionStringBuilder.createWithAadAccessTokenAuthentication("https://statusreturner.azurewebsites.net/nocloud/" + code, "token"));
                try {
                    client.executeQuery("db", "table");
                    Assertions.fail("Expected exception");
                } catch (DataServiceException e) {
                    Assertions.assertTrue(e.getMessage().contains("" + code));
                    Assertions.assertTrue(e.getMessage().contains("metadata"));
                }
            } catch (Exception e) {
                return e;
            }
            return null;
        }).forEach(e -> {
            if (e != null) {
                Assertions.fail(e);
            }
        });
    }

    @Test
    void testNoRedirectsClientFail() {
        KustoTrustedEndpoints.addTrustedHosts(Collections.singletonList(new MatchRule("statusreturner.azurewebsites.net", false)), false);
        List<Integer> redirectCodes = Arrays.asList(301, 302, 307, 308);
        redirectCodes.parallelStream().map(code -> {
            try {
                Client client = ClientFactory.createClient(
                        ConnectionStringBuilder.createWithAadAccessTokenAuthentication("https://statusreturner.azurewebsites.net/" + code, "token"));
                try {
                    client.executeQuery("db", "table");
                    Assertions.fail("Expected exception");
                } catch (DataServiceException e) {
                    Assertions.assertTrue(e.getMessage().contains("" + code));
                    Assertions.assertFalse(e.getMessage().contains("metadata"));
                } catch (Exception e) {
                    return e;
                }
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            return null;
        }).forEach(e -> {
            if (e != null) {
                Assertions.fail(e);
            }
        });
    }

    @Test
    void testStreamingIngestFromBlob() throws IngestionClientException, IngestionServiceException, IOException {
        try (ResourceManager resourceManager = new ResourceManager(dmCslClient, null)) {
            ContainerWithSas container = resourceManager.getShuffledContainers().get(0);
            AzureStorageClient azureStorageClient = new AzureStorageClient();

            for (TestDataItem item : dataForTests) {
                if (item.testOnstreamingIngestion) {
                    String blobName = String.format("%s__%s.%s.gz",
                            "testStreamingIngestFromBlob",
                            UUID.randomUUID(),
                            item.ingestionProperties.getDataFormat());

                    String blobPath = container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas();

                    azureStorageClient.uploadLocalFileToBlob(item.file, blobName,
                            container.getContainer(), !item.file.getName().endsWith(".gz"));
                    try {
                        streamingIngestClient.ingestFromBlob(new BlobSourceInfo(blobPath), item.ingestionProperties);
                    } catch (Exception ex) {
                        Assertions.fail(ex);
                    }
                    assertRowCount(item.rows, false);
                }
            }
        }
    }
}
