package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.ingest.IngestionMapping.*;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT;
import com.microsoft.azure.kusto.ingest.source.*;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.*;

public class E2ETest {
    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ClientImpl queryClient;
    private static String databaseName;
    private static String principalFqn;
    private static String resourcesPath;
    private static int currentCount = 0;
    private static List<TestDataItem> dataForTests;
    private static final String tableName = "JavaTest" + System.currentTimeMillis() / 1000L;;
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";

    @BeforeAll
    public static void setup() {
        databaseName = System.getenv("TEST_DATABASE");
        String appId = System.getenv("APP_ID");
        String appKey = System.getenv("APP_KEY");
        String tenantId = System.getenv("TENANT_ID");

        // test - delete!!!
        System.out.println("db");
        System.out.println(databaseName);
        System.out.flush();
        System.out.println("cs");
        System.out.println(System.getenv("ENGINE_CONECTION_STRING"));
        System.out.flush();
        /// end

        principalFqn = String.format("aadapp=%s;%s", appId, tenantId);

        ConnectionStringBuilder dmCsb = ConnectionStringBuilder
                .createWithAadApplicationCredentials(System.getenv("ENGINE_CONECTION_STRING"), appId, appKey, tenantId);
        try {
            ingestClient = IngestClientFactory.createClient(dmCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create ingest client", ex);
        }

        ConnectionStringBuilder engineCsb = ConnectionStringBuilder
                .createWithAadApplicationCredentials(System.getenv("ENGINE_CONECTION_STRING"), appId, appKey, tenantId);
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(engineCsb);
            queryClient = new ClientImpl(engineCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamingIngest client", ex);
        }

        createTableAndMapping();
        createTestData();
    }

    private static void createTableAndMapping() {
        try {
            queryClient.execute(databaseName, String.format(".create table %s %s", tableName, tableColumns));
        } catch (Exception ex) {
            Assertions.fail("Failed to create new table", ex);
        }

        resourcesPath = Paths.get(System.getProperty("user.dir"), "src","test", "resources").toString();
        try {
            String mappingAsString = new String(Files.readAllBytes(Paths.get(resourcesPath, "dataset_mapping.json")));
            queryClient.execute(databaseName, String.format(".create table %s ingestion json mapping '%s' '%s'",
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
        ColumnMapping[] columnMapping = new ColumnMapping[] { first, second };
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

    private void assertRowCount(int expectedRowsCount) {
        KustoOperationResult result = null;
        int timeoutInSec = 100;
        int actualRowsCount = 0;

        while (timeoutInSec > 0) {
            try {
                Thread.sleep(5000);
                timeoutInSec -= 5;

                result = queryClient.execute(databaseName, String.format("%s | count", tableName));
            } catch (Exception ex) {
                continue;
            }
            KustoResultSetTable mainTableResult = result.getPrimaryResults();
            mainTableResult.next();
            actualRowsCount = mainTableResult.getInt(0) - currentCount;
            if (actualRowsCount >= expectedRowsCount) {
                break;
            }
        }
        currentCount += actualRowsCount;
        assertEquals(expectedRowsCount, actualRowsCount);
    }

    @Test
    void testShowPrincipals() {
        KustoOperationResult result = null;
        boolean found = false;
        try {
            result = queryClient.execute(databaseName, String.format(".show database %s principals", databaseName));
        } catch (Exception ex) {
            Assertions.fail("Failed to execute show database principal command", ex);
        }
        KustoResultSetTable mainTableResultSet= result.getPrimaryResults();
        while (mainTableResultSet.next()){
            if (mainTableResultSet.getString("PrincipalFQN").equals(principalFqn)) {
                found = true;
            }
        }

        Assertions.assertTrue(found, "Faile to find authorized AppId in the database principals");
    }

    @Test
    void testIngestFromFile() {
        for (TestDataItem item : dataForTests) {
            FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(),item.file.length());
            try {
                ingestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(item.rows);
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
            assertRowCount(item.rows);
        }
    }

    @Test
    void testStramingIngestFromFile() {
        for (TestDataItem item : dataForTests) {
            if (item.testOnstreamingIngestion) {
                FileSourceInfo fileSourceInfo = new FileSourceInfo(item.file.getPath(),item.file.length());
                try {
                    streamingIngestClient.ingestFromFile(fileSourceInfo, item.ingestionProperties);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                assertRowCount(item.rows);
            }
        }
    }

    @Test
    void testStramingIngestFromStream() throws FileNotFoundException {
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
                assertRowCount(item.rows);
            }
        }
    }

    @AfterAll
    public static void tearDown() {
        try {
            queryClient.execute(databaseName, String.format(".drop table %s ifexists", tableName));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop table", ex);
        }
    }
}