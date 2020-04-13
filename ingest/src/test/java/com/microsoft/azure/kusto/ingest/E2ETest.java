package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.ingest.IngestionMapping.IngestionMappingKind;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.nio.file.Files.readAllBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class E2ETest {
    private static IngestClient ingestClient;
    private static StreamingIngestClient streamingIngestClient;
    private static ClientImpl queryClient;
    private static String databaseName;
    private static String principalFqn;
    private static String resourcesPath;
    private static ColumnMapping[] columnMapping;
    private static int currentCount = 0;
    private static List<TestData> dataForTests;
    private static final String tableName = "javaTest";
    private static final String mappingReference = "mappingRef";
    private static final String tableColumns = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)";

    @BeforeAll
    public static void setUp() {
        databaseName = "ClientTest";// System.getenv("TEST_DATABASE");
        String appId = "b699d721-4f6f-4320-bc9a-88d578dfe68f"; // System.getenv("APP_ID"),
        String appKey = "UJImT=AzMK-If:4Dg964BYbhHDflDjQ["; // System.getenv("APP_KEY"),
        String tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47"; // System.getenv("TENANT_ID"));

        principalFqn = String.format("aadapp=%s;%s", appId, tenantId);

        ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                "https://ingest-amitsh.dev.kusto.windows.net", // System.getenv("ENGINE_CONECTION_STRING"),
                appId, appKey, tenantId);
        try {
            ingestClient = IngestClientFactory.createClient(dmCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create ingest client", ex);
        }

        ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                "https://amitsh.dev.kusto.windows.net", // System.getenv("ENGINE_CONECTION_STRING"),
                appId, appKey, tenantId);
        try {
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(engineCsb);
            queryClient = new ClientImpl(engineCsb);
        } catch (URISyntaxException ex) {
            Assertions.fail("Failed to create query and streamin client", ex);
        }
        try {
            queryClient.execute(databaseName, String.format(".drop table %s ifexists", tableName));
            Thread.sleep(1000);
            queryClient.execute(databaseName, String.format(".create table %s %s", tableName, tableColumns));
        } catch (Exception ex) {
            Assertions.fail("Failed to drop and create new table", ex);
        }
        resourcesPath = System.getProperty("user.dir") + "\\src\\test\\resources\\";

        try {
            String mappingAsString = new String(readAllBytes(Path.of(resourcesPath, "dataset_mapping.json")));
            queryClient.execute(databaseName, String.format(".create table %s ingestion json mapping '%s' '%s'",
                    tableName, mappingReference, mappingAsString));
        } catch (Exception ex) {
            Assertions.fail("Failed to create ingestion mapping", ex);
        }

        createTestData();
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
        ingestionPropertiesWithMappingReference.setDataFormat(DATA_FORMAT.json);
        ColumnMapping first = new ColumnMapping("rownumber", "int");
        first.setPath("$.rownumber");
        ColumnMapping second = new ColumnMapping("rowguid", "string");
        second.setPath("$.rowguid");
        ColumnMapping[] columnMapping = new ColumnMapping[]{first, second};
        ingestionPropertiesWithColumnMapping.setIngestionMapping(columnMapping, IngestionMappingKind.Json);

        dataForTests = Arrays.asList(new TestData() {
            {
                path = resourcesPath + "dataset.csv";
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        }, new TestData() {
            {
                path = resourcesPath + "dataset.csv.gz";
                rows = 10;
                ingestionProperties = ingestionPropertiesWithoutMapping;
            }
        }, new TestData() {
            {
                path = resourcesPath + "dataset.json";
                rows = 2;
                ingestionProperties = ingestionPropertiesWithMappingReference;
            }
        }, new TestData() {
            {
                path = resourcesPath + "dataset.json.gz";
                rows = 2;
                ingestionProperties = ingestionPropertiesWithMappingReference;
            }
        }, new TestData() {
            {
                path = resourcesPath + "dataset.json";
                rows = 2;
                ingestionProperties = ingestionPropertiesWithColumnMapping;
            }
        });
    }

    private void assertRowCount(int expectedRowsCount) {
        KustoOperationResult result = null;
        int timeoutInsec = 300;
        int actualRowscount = 0;

        while (timeoutInsec > 0) {
            try {
                Thread.sleep(10000);
                timeoutInsec -= 10;

                result = queryClient.execute(databaseName, String.format("%s | count", tableName));
            } catch (Exception ex) {
                continue;
            }
            actualRowscount = (int) result.getPrimaryResults().getData().get(0).get(0) - currentCount;
            if (actualRowscount >= expectedRowsCount) {
                break;
            }
        }
        currentCount += actualRowscount;
        assertEquals(expectedRowsCount, actualRowscount);
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

        for (ArrayList<Object> row : result.getPrimaryResults().getData()) {
            if (row.get(4).equals(principalFqn)) {
                found = true;
            }
        }

        Assertions.assertTrue(found, "Faile to find authorized AppId in the database principals");
    }

    @Test
    void testIngestFromFile() {
        for (TestData data : dataForTests) {
            FileSourceInfo fileSourceInfo = new FileSourceInfo(data.path, new File(data.path).length());
            try {
                ingestClient.ingestFromFile(fileSourceInfo, data.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(data.rows);
        }
    }

    @Test
    void testIngestFromStream() throws FileNotFoundException {
        for (TestData data : dataForTests) {
            File file = new File(data.path);
            InputStream stream = new FileInputStream(file);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);
            if (data.path.endsWith(".gz")) {
                streamSourceInfo.setCompressionType(CompressionType.gz);
            }
            try {
                ingestClient.ingestFromStream(streamSourceInfo, data.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(data.rows);
        }
    }

    @Test
    void testStramingIngestFromFile() {
        for (TestData data : dataForTests) {
            if(data.ingestionProperties.getIngestionMapping().getColumnMappings().length > 0){
                continue; // straming ingest doesn't suport inline mapping
            }
            FileSourceInfo fileSourceInfo = new FileSourceInfo(data.path, new File(data.path).length());
            try {
                streamingIngestClient.ingestFromFile(fileSourceInfo, data.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(data.rows);
        }
    }

    @Test
    void testStramingIngestFromStream() throws FileNotFoundException {
        for (TestData data : dataForTests) {
            if(data.ingestionProperties.getIngestionMapping().getColumnMappings().length > 0){
                continue; // straming ingest doesn't suport inline mapping
            }
            File file = new File(data.path);
            InputStream stream = new FileInputStream(file);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream);
            if (data.path.endsWith(".gz")) {
                streamSourceInfo.setCompressionType(CompressionType.gz);
            }
            try {
                streamingIngestClient.ingestFromStream(streamSourceInfo, data.ingestionProperties);
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            assertRowCount(data.rows);
        }
    }
}

class TestData {
    public String path;
    public IngestionProperties ingestionProperties;
    public int rows;
}