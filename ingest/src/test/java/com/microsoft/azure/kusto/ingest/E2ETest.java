// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

class E2ETest {
    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ClientImpl queryClient;
    private static final String databaseName = System.getenv("TEST_DATABASE");
    private static final String appId = System.getenv("APP_ID");
    private static final String appKey = System.getenv("APP_KEY");
    private static final String tenantId = System.getenv().getOrDefault("TENANT_ID","microsoft.com");
    private static String principalFqn;
    private static String resourcesPath;
    private static int currentCount = 0;
    private static List<TestDataItem> dataForTests;
    private static final String tableName = "JavaTest";
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";

    @BeforeAll
    public static void setUp() {
        principalFqn = String.format("aadapp=%s;%s", appId, tenantId);

        ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("DM_CONNECTION_STRING"), appId, appKey, tenantId);
        try {
            ingestClient = IngestClientFactory.createClient(dmCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create ingest client", ex);
        }

        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("ENGINE_CONNECTION_STRING"), appId, appKey, tenantId);
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(engineCsb);
            queryClient = new ClientImpl(engineCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
        }

        CreateTableAndMapping();
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

    private static void CreateTableAndMapping() {
        try {
            queryClient.executeToJsonResult(databaseName, String.format(".drop table %s ifexists", tableName));
        } catch (Exception ex) {
        }
        try {
            Thread.sleep(2000);
            queryClient.executeToJsonResult(databaseName, String.format(".create table %s %s", tableName, tableColumns));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop and create new table", ex);
        }

        resourcesPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources").toString();
        try {
            Thread.sleep(2000);
            String mappingAsString = new String(Files.readAllBytes(Paths.get(resourcesPath, "dataset_mapping.json")));
            queryClient.executeToJsonResult(databaseName, String.format(".create table %s ingestion json mapping '%s' '%s'",
                    tableName, mappingReference, mappingAsString));
        } catch (Exception ex) {
            Assertions.fail("Failed to create ingestion mapping", ex);
        }
    }

    private static void createTestData() {
        IngestionProperties ingestionPropertiesWithoutMapping = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithoutMapping.setFlushImmediately(true);

        IngestionProperties ingestionPropertiesWithMappingReference = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithMappingReference.setFlushImmediately(true);
        ingestionPropertiesWithMappingReference.setIngestionMapping(mappingReference, IngestionMappingKind.Json);
        ingestionPropertiesWithMappingReference.setDataFormat(DATA_FORMAT.json);

        IngestionProperties ingestionPropertiesWithColumnMapping = new IngestionProperties(databaseName, tableName);
        ingestionPropertiesWithColumnMapping.setFlushImmediately(true);
        ingestionPropertiesWithColumnMapping.setDataFormat(DATA_FORMAT.json);
        ColumnMapping first = new ColumnMapping("rownumber", "int");
        first.setPath("$.rownumber");
        ColumnMapping second = new ColumnMapping("rowguid", "string");
        second.setPath("$.rowguid");
        ColumnMapping[] columnMapping = new ColumnMapping[]{first, second};
        ingestionPropertiesWithColumnMapping.setIngestionMapping(columnMapping, IngestionMappingKind.Json);

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
        int timeoutInSec = 100;
        int actualRowsCount = 0;

        while (timeoutInSec > 0) {
            try {
                Thread.sleep(5000);
                timeoutInSec -= 5;

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
            //result = localQueryClient.execute(databaseName, String.format(".show version"));
        } catch (Exception ex) {
            Assertions.fail("Failed to execute show database principal command", ex);
        }
        KustoResultSetTable mainTableResultSet = result.getPrimaryResults();
        while (mainTableResultSet.next()) {
            if (mainTableResultSet.getString("PrincipalFQN").equals(principalFqn)) {
                found = true;
            }
        }
        return found;
    }

    @Test
    void testIngestFromFile() {
        for (TestDataItem item : dataForTests) {
            FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(), item.file.length());
            try {
                ingestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(item.rows, false);
        }
    }

    @Test
    void testIngestFromStream() throws FileNotFoundException {
        for (TestDataItem item : dataForTests) {
            InputStream stream = new FileInputStream(item.file);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);
            if (item.file.getPath().endsWith(".gz")) {
                streamSourceInfo.setCompressionType(CompressionType.gz);
            }
            try {
                ingestClient.ingestFromStream(streamSourceInfo, item.ingestionProperties);
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
            if (item.testOnstreamingIngestion) {
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

    private boolean hasAuthentication(ConnectionStringBuilder engineCsb) {
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
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithUserPrompt(System.getenv("ENGINE_CONNECTION_STRING"), null, System.getenv("USERNAME_HINT"));
        assertTrue(hasAuthentication(engineCsb));
    }

    @Test
    @Disabled("This is an interactive approach. Remove this line to test manually.")
    void testCreateWithDeviceAuthentication() {
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithDeviceCode(System.getenv("ENGINE_CONNECTION_STRING"), null);
        assertTrue(hasAuthentication(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCertificate() throws GeneralSecurityException, IOException {
        X509Certificate cer = SecurityUtils.getPublicCertificate(System.getenv("PUBLIC_X509CER_FILE_LOC"));
        PrivateKey privateKey = SecurityUtils.getPrivateKey(System.getenv("PRIVATE_PKCS8_FILE_LOC"));
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCertificate(System.getenv("ENGINE_CONNECTION_STRING"), appId, cer, privateKey, "microsoft.onmicrosoft.com");
        assertTrue(hasAuthentication(engineCsb));
    }

    @Test
    void testCreateWithAadApplicationCredentials() {
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getenv("ENGINE_CONNECTION_STRING"), appId, appKey, tenantId);
        assertTrue(hasAuthentication(engineCsb));
    }

    @Test
    void testCreateWithAadAccessTokenAuthentication() {
        String token = System.getenv("TOKEN");
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(System.getenv("ENGINE_CONNECTION_STRING"), token);
        assertTrue(hasAuthentication(engineCsb));
    }

    @Test
    void testCreateWithAadTokenProviderAuthentication() {
        Callable<String> tokenProviderCallable = () -> System.getenv("TOKEN");
        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadTokenProviderAuthentication(System.getenv("ENGINE_CONNECTION_STRING"), tokenProviderCallable);
        assertTrue(hasAuthentication(engineCsb));
    }
}