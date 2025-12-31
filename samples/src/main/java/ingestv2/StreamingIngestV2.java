// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.StreamingIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Sample demonstrating streaming ingestion using the new ingest-v2 API.
 * This is the modern API that uses Kotlin-based clients with coroutines,
 * providing better async support and a cleaner API design.
 */
public class StreamingIngestV2 {

    private static String database;
    private static String table;
    private static String mapping;
    private static StreamingIngestClient streamingIngestClient;

    public static void main(String[] args) {
        try {
            // Get configuration from system properties
            String engineEndpoint = System.getProperty("clusterPath"); // "https://<cluster>.kusto.windows.net"
            String appId = System.getProperty("app-id");
            String appKey = System.getProperty("appKey");
            String tenant = System.getProperty("tenant");

            database = System.getProperty("dbName");
            table = System.getProperty("tableName");
            mapping = System.getProperty("dataMappingName");

            ChainedTokenCredential credential;

            // Create Azure AD credential
            if(StringUtils.isNotBlank(appId) && StringUtils.isNotBlank(appKey) && StringUtils.isNotBlank(tenant)) {
                credential = new ChainedTokenCredentialBuilder()
                        .addFirst(new ClientSecretCredentialBuilder()
                                .clientId(appId)
                                .clientSecret(appKey)
                                .tenantId(tenant)
                                .build())
                        .build();
            } else {
                credential = new ChainedTokenCredentialBuilder()
                        .addFirst(new AzureCliCredentialBuilder().build())
                        .build();
            }

            // Create streaming ingest client using the new v2 API
            streamingIngestClient = StreamingIngestClientBuilder.create(engineEndpoint)
                    .withAuthentication(credential)
                    .build();

            System.out.println("Streaming Ingest Client created successfully");

            // Run ingestion examples
            ingestFromStream();
            ingestFromFile();

            System.out.println("All ingestion operations completed");

        } catch (Exception e) {
            System.err.println("Error during ingestion: " + e.getMessage());
        }
    }

    /**
     * Demonstrates ingestion from various stream sources including:
     * - In-memory string data as CSV
     * - Compressed file stream (CSV)
     * - JSON file stream with mapping
     */
    static void ingestFromStream() throws Exception {
        System.out.println("\n=== Ingesting from Streams ===");

        // Example 1: Ingest from in-memory CSV string
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(
                csvInputStream, Format.csv, CompressionType.NONE,
                UUID.randomUUID(), "csv-test-src", false);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting CSV data from string...");
        ExtendedIngestResponse ingestResponse = streamingIngestClient.ingestAsync(csvStreamSource, csvProperties).get();
        System.out.println("CSV ingestion completed. Operation ID: " + ingestResponse.getIngestResponse().getIngestionOperationId());

        // Example 2: Ingest from compressed CSV file
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        FileInputStream compressedCsvStream = new FileInputStream(resourcesDirectory + "dataset.csv.gz");

        StreamSource compressedStreamSource = new StreamSource(
                compressedCsvStream,
                Format.csv, CompressionType.GZIP,
                UUID.randomUUID(),
                "compressed-csv-stream",
                false
        );
        System.out.println("Ingesting compressed CSV file...");
        ExtendedIngestResponse compressedResponse = streamingIngestClient.ingestAsync(compressedStreamSource, csvProperties).get();
        System.out.println("Compressed CSV ingestion completed. Operation ID: " + compressedResponse.getIngestResponse().getIngestionOperationId());
        compressedCsvStream.close();

        // Example 3: Ingest JSON with mapping
        FileInputStream jsonStream = new FileInputStream(resourcesDirectory + "dataset.json");

        StreamSource jsonStreamSource = new StreamSource(
                jsonStream,
                Format.json, CompressionType.NONE,
                UUID.randomUUID(),
                "json-data-stream",
                false
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = streamingIngestClient.ingestAsync(jsonStreamSource, jsonProperties).get();
        System.out.println("JSON ingestion completed. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
        jsonStream.close();
    }

    /**
     * Demonstrates ingestion from file sources including:
     * - CSV file
     * - Compressed JSON file with mapping
     */
    static void ingestFromFile() throws Exception {
        System.out.println("\n=== Ingesting from Files ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest CSV file
        FileSource csvFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.NONE,
                "jcsv-file-source"
        );

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting CSV file...");
        ExtendedIngestResponse csvResponse = streamingIngestClient.ingestAsync(csvFileSource, csvProperties).get();
        System.out.println("CSV file ingestion completed. Operation ID: " + csvResponse.getIngestResponse().getIngestionOperationId());

        // Example 2: Ingest compressed JSON file with mapping
        FileSource jsonFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP,
                "sjson-compressed-file"
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting compressed JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = streamingIngestClient.ingestAsync(jsonFileSource, jsonProperties).get();
        System.out.println("Compressed JSON file ingestion completed. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
    }
}

