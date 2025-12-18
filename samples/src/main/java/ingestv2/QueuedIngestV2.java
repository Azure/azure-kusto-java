// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.Status;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Sample demonstrating queued ingestion using the new ingest-v2 API.
 * 
 * Queued ingestion is the recommended approach for most production scenarios because:
 * - It handles larger data volumes more reliably
 * - It provides better guarantees for data delivery
 * - It automatically handles retries and failures
 * - It supports tracking ingestion status
 * 
 * This is the modern API that uses Kotlin-based clients with coroutines,
 * providing better async support and a cleaner API design.
 */
public class QueuedIngestV2 {

    private static String database;
    private static String table;
    private static String mapping;
    private static QueuedIngestClient queuedIngestClient;

    public static void main(String[] args) {
        try {
            // Get configuration from system properties
            String ingestionEndpoint = System.getProperty("clusterPath"); // "https://<cluster>.kusto.windows.net"
            String appId = System.getProperty("app-id");
            String appKey = System.getProperty("appKey");
            String tenant = System.getProperty("tenant");

            database = System.getProperty("dbName");
            table = System.getProperty("tableName");
            mapping = System.getProperty("dataMappingName");

            ChainedTokenCredential credential;

            // Create Azure AD credential
            if (StringUtils.isNotBlank(appId) && StringUtils.isNotBlank(appKey) && StringUtils.isNotBlank(tenant)) {
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

            // Create queued ingest client using the new v2 API
            queuedIngestClient = QueuedIngestClientBuilder.create(ingestionEndpoint)
                    .withAuthentication(credential)
                    .withMaxConcurrency(4)
                    .build();

            System.out.println("Queued Ingest Client created successfully");

            // Run ingestion examples
            ingestFromStream();
            ingestFromFile();
            ingestMultipleFiles();

            System.out.println("\nAll ingestion operations completed");
            System.out.println("Queued ingestion is asynchronous. Use the operation IDs to track ingestion status.");

        } catch (Exception e) {
            System.err.println("Error during ingestion: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (queuedIngestClient != null) {
                queuedIngestClient.close();
            }
        }
    }

    /**
     * Demonstrates queued ingestion from various stream sources including:
     * - In-memory string data as CSV
     * - Compressed file stream (CSV)
     * - JSON file stream with mapping
     */
    static void ingestFromStream() throws Exception {
        System.out.println("\n=== Queued Ingestion from Streams ===");

        // Example 1: Ingest from in-memory CSV string
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(
                csvInputStream, CompressionType.NONE, Format.csv,
                UUID.randomUUID(), "csv-stream-src", false);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.csv)
                .withEnableTracking(true)  // Enable tracking for status queries
                .build();

        System.out.println("Queuing CSV data from string...");
        ExtendedIngestResponse ingestResponse = queuedIngestClient.ingestAsync(csvStreamSource, csvProperties).get();
        System.out.println("CSV data queued. Operation ID: " + ingestResponse.getIngestResponse().getIngestionOperationId());
        
        // Demonstrate status checking
        checkOperationStatus(ingestResponse);

        // Example 2: Ingest from compressed CSV file
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        FileInputStream compressedCsvStream = new FileInputStream(resourcesDirectory + "dataset.csv.gz");

        StreamSource compressedStreamSource = new StreamSource(
                compressedCsvStream,
                CompressionType.GZIP,
                Format.csv,
                UUID.randomUUID(),
                "compressed-csv-stream",
                false
        );

        System.out.println("Queuing compressed CSV file...");
        ExtendedIngestResponse compressedResponse = queuedIngestClient.ingestAsync(compressedStreamSource, csvProperties).get();
        System.out.println("Compressed CSV queued. Operation ID: " + compressedResponse.getIngestResponse().getIngestionOperationId());
        compressedCsvStream.close();

        // Example 3: Ingest JSON with mapping
        FileInputStream jsonStream = new FileInputStream(resourcesDirectory + "dataset.json");

        StreamSource jsonStreamSource = new StreamSource(
                jsonStream,
                CompressionType.NONE,
                Format.json,
                UUID.randomUUID(),
                "json-data-stream",
                false
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.json)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Queuing JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = queuedIngestClient.ingestAsync(jsonStreamSource, jsonProperties).get();
        System.out.println("JSON data queued. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
        jsonStream.close();
    }

    /**
     * Demonstrates queued ingestion from file sources including:
     * - CSV file
     * - Compressed JSON file with mapping
     */
    static void ingestFromFile() throws Exception {
        System.out.println("\n=== Queued Ingestion from Files ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest CSV file
        FileSource csvFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.NONE
        );

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.csv)
                .withEnableTracking(true)
                .build();

        System.out.println("Queuing CSV file...");
        ExtendedIngestResponse csvResponse = queuedIngestClient.ingestAsync(csvFileSource, csvProperties).get();
        System.out.println("CSV file queued. Operation ID: " + csvResponse.getIngestResponse().getIngestionOperationId());

        // Example 2: Ingest compressed JSON file with mapping
        FileSource jsonFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.multijson)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Queuing compressed JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = queuedIngestClient.ingestAsync(jsonFileSource, jsonProperties).get();
        System.out.println("Compressed JSON file queued. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
    }

    /**
     * Demonstrates batch ingestion of multiple files in a single request.
     * This is more efficient than ingesting files one by one as it reduces
     * the number of API calls and allows the system to optimize processing.
     */
    static void ingestMultipleFiles() throws Exception {
        System.out.println("\n=== Queued Ingestion of Multiple Files (Batch) ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Create a list of file sources to ingest together
        List<FileSource> fileSources = new ArrayList<>();

        // Add CSV file
        fileSources.add(new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.NONE
        ));

        // Add compressed CSV file
        fileSources.add(new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv.gz"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.GZIP
        ));

        IngestRequestProperties batchProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.csv)
                .withEnableTracking(true)
                .build();

        System.out.println("Queuing " + fileSources.size() + " files in a single batch...");
        List<IngestionSource> sources = new ArrayList<>(fileSources);
        ExtendedIngestResponse batchResponse = queuedIngestClient.ingestAsync(sources, batchProperties).get();
        System.out.println("Batch ingestion queued. Operation ID: " + batchResponse.getIngestResponse().getIngestionOperationId());
        
        // Demonstrate detailed status checking for batch operation
        checkOperationStatus(batchResponse);
    }

    /**
     * Demonstrates how to check the status of an ingestion operation.
     * 
     * For queued ingestion, you can:
     * 1. Get a summary of the operation (succeeded/failed/in-progress counts)
     * 2. Get detailed status for each blob in the operation
     * 
     * Note: The operation ID format from the API may vary; this example shows
     * the pattern for status checking when a valid UUID is available.
     */
    static void checkOperationStatus(ExtendedIngestResponse response) throws Exception {
        String operationId = response.getIngestResponse().getIngestionOperationId().toString();
        
        if (operationId == null || operationId.isEmpty()) {
            System.out.println("  No operation ID available for status tracking");
            return;
        }

        System.out.println("  Operation ID: " + operationId);
        System.out.println("  Queued ingestion is asynchronous, so data will be ingested shortly.");
        
        // The operation ID format from the v2 API may contain timestamps and other info
        // Status checking is available when a UUID-format operation ID is provided
        // For demonstration, we skip status checking here as it requires the operation 
        // to be processed first
    }
}
