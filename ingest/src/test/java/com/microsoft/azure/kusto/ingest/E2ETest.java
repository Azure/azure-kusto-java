// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracer;
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
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporters.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import org.apache.http.conn.util.InetAddressUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
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
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static Client queryClient;
    private static Client dmCslClient;
    private static StreamingClient streamingClient;
    private static final String databaseName = System.getenv("TEST_DATABASE");
    private static final String appId = System.getenv("APP_ID");
    private static final String appKey = System.getenv("APP_KEY");
    private static final String tenantId = System.getenv().getOrDefault("TENANT_ID", "microsoft.com");
    private static String principalFqn;
    private static String resourcesPath;
    private static int currentCount = 0;
    private static List<TestDataItem> dataForTests;
    private static String tableName;
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";
    private final ObjectMapper objectMapper = Utils.getObjectMapper();
    public static boolean enabled = false;

    private static void initializeTracing() {
        if (!enabled) {
            enabled = true;
            enableDistributedTracing();
            Tracer.initializeTracer(new OpenTelemetryTracer());
        }
    }

    private static void enableDistributedTracing() {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "sdkE2E")));
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(new LoggingSpanExporter()).build())
                .setResource(resource)
                .build();
        OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .buildAndRegisterGlobal();
    }

    @BeforeAll
    public static void setUp() {
        initializeTracing();
        tableName = "JavaTest_" + new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS").format(Calendar.getInstance().getTime()) + "_"
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        principalFqn = String.format("aadapp=%s;%s", appId, tenantId);

        ConnectionStringBuilder dmCsb = createConnection(System.getenv("DM_CONNECTION_STRING"));
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

        ConnectionStringBuilder engineCsb = createConnection(System.getenv("ENGINE_CONNECTION_STRING"));
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
        if (appKey == null || appKey.isEmpty()) {
            return ConnectionStringBuilder.createWithAzureCli(connectionString);
        }

        return ConnectionStringBuilder.createWithAadApplicationCredentials(connectionString, appId, appKey,
                tenantId);
    }

    @AfterAll
    public static void tearDown() {
        try {
            log.info("starting tearDown");
            queryClient.executeToJsonResult(databaseName, String.format(".drop table %s ifexists", tableName));
            log.info("tearDown after exec");

            ingestClient.close();
            log.info("tearDown after close ing");

            managedStreamingIngestClient.close();
            log.info("tearDown after close manage");

            queryClient.close();
            log.info("finished tearDown");

        } catch (Exception ex) {
            Assertions.fail("Failed to drop table", ex);
        }
    }

    private static boolean isManualExecution() {
        return false;
        // return System.getenv("CI_EXECUTION") == null || !System.getenv("CI_EXECUTION").equals("1");
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

        IngestionProperties ingestionPropertiesWithIgnoreFirstRecord = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithIgnoreFirstRecord.setFlushImmediately(true);
        ingestionPropertiesWithIgnoreFirstRecord.setDataFormat(DataFormat.CSV);
        ingestionPropertiesWithIgnoreFirstRecord.setIgnoreFirstRecord(true);

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

        IngestionProperties ingestionPropertiesWithTableReportMethod = new IngestionProperties(databaseName, tableName);
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
                    String result = queryClient.executeToJsonResult(databaseName, String.format("%s | count", tableName));
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

    private boolean isDatabasePrincipal(Client localQueryClient) {
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
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithUserPrompt(System.getenv("ENGINE_CONNECTION_STRING"), null,
                System.getenv("USERNAME_HINT"));
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithConnectionStringAndUserPrompt() {
        Assumptions.assumeTrue(isManualExecution());
        ConnectionStringBuilder engineCsb = new ConnectionStringBuilder(
                "Data Source=" + System.getenv("ENGINE_CONNECTION_STRING") + ";User ID=" + System.getenv("USERNAME_HINT"));
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithDeviceAuthentication() {
        Assumptions.assumeTrue(isManualExecution());
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
        Assumptions.assumeTrue(appKey != null);
        ConnectionStringBuilder engineCsb = createConnection(System.getenv("ENGINE_CONNECTION_STRING"));
        assertTrue(canAuthenticate(engineCsb));
    }

    @Test
    void testCreateWithConnectionStringAndAadApplicationCredentials() {
        Assumptions.assumeTrue(appKey != null);
        ConnectionStringBuilder engineCsb = new ConnectionStringBuilder(
                "Data Source=" + System.getenv("ENGINE_CONNECTION_STRING") + ";AppClientId=" + appId + ";AppKey=" + appKey + ";Authority ID=" + tenantId);
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
        try (InputStream is = streamingClient.executeStreamingQuery(databaseName, query, clientRequestProperties);
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
    void testSameHttpClientInstance() throws DataClientException, DataServiceException, URISyntaxException, IOException {
        ConnectionStringBuilder engineCsb = createConnection(System.getenv("ENGINE_CONNECTION_STRING"));
        HttpClient httpClient = HttpClientFactory.create(null);
        HttpClient httpClientSpy = Mockito.spy(httpClient);
        Client clientImpl = ClientFactory.createClient(engineCsb, httpClientSpy);

        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        String query = tableName + " | take 1000";

        clientImpl.execute(databaseName, query, clientRequestProperties);
        clientImpl.execute(databaseName, query, clientRequestProperties);

        // Todo potentially need a try with resources here
        Mockito.verify(httpClientSpy, atLeast(2)).sendSync(any(), eq(Context.NONE));
        clientImpl.close();
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
                    client.execute("db", "table");
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
                    client.execute("db", "table");
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
