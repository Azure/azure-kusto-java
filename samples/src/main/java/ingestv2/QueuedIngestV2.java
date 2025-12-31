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
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.Status;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sample demonstrating queued ingestion using the new ingest-v2 API.
 * This is the modern API that uses Kotlin-based clients with coroutines,
 * providing better async support and a cleaner API design.
 * Queued ingestion is asynchronous and provides reliable, high-throughput
 * data ingestion with operation tracking capabilities.
 */
public class QueuedIngestV2 {

    private static String database;
    private static String table;
    private static String mapping;
    private static QueuedIngestClient queuedIngestClient;

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

            if(engineEndpoint == null || engineEndpoint.isEmpty()) {
                throw new IllegalArgumentException("Cluster endpoint (clusterPath) must be provided as a system property.");
            }

            // Create queued ingest client using the new v2 API
            queuedIngestClient = QueuedIngestClientBuilder.create(engineEndpoint)
                    .withAuthentication(credential)
                    .withMaxConcurrency(10)  // Set maximum concurrent uploads
                    .build();

            System.out.println("Queued Ingest Client created successfully");

            // Collect all futures for non-blocking execution
            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            // Run ingestion examples
            allFutures.addAll(ingestFromStream());
            allFutures.addAll(ingestFromFile());
            allFutures.add(ingestMultipleSources());

            // Wait for all operations to complete
            CompletableFuture<Void> allOf = CompletableFuture.allOf(
                    allFutures.toArray(new CompletableFuture[0])
            );

            System.out.println("\nWaiting for all ingestion operations to complete...");
            allOf.get(5, TimeUnit.MINUTES);

            System.out.println("\nAll ingestion operations completed successfully!");

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
     * Demonstrates ingestion from various stream sources including:
     * - In-memory string data as CSV
     * - Compressed file stream (CSV)
     * - JSON file stream with mapping
     */
    static List<CompletableFuture<Void>> ingestFromStream() throws Exception {
        System.out.println("\n=== Queued Ingestion from Streams ===");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Example 1: Ingest from in-memory CSV string
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(
                csvInputStream, Format.csv, CompressionType.NONE,
                UUID.randomUUID(), "csv-queued-stream", false);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing CSV data from string...");
        CompletableFuture<Void> csvFuture = queuedIngestClient.ingestAsync(csvStreamSource, csvProperties)
                .thenCompose(response -> {
                    System.out.println("CSV ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestionOperation(response, "CSV Stream");
                })
                .whenComplete((unused, throwable) -> closeQuietly(csvInputStream));
        futures.add(csvFuture);

        // Example 2: Ingest from compressed CSV file
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        InputStream compressedCsvStream = new ByteArrayInputStream(readResourceBytes(resourcesDirectory, "dataset.csv.gz"));

        StreamSource compressedStreamSource = new StreamSource(
                compressedCsvStream,
                Format.csv, CompressionType.GZIP,
                UUID.randomUUID(),
                "compressed-csv-queued-stream",
                false
        );

        System.out.println("Queueing compressed CSV file...");
        CompletableFuture<Void> compressedFuture = queuedIngestClient.ingestAsync(compressedStreamSource, csvProperties)
                .thenCompose(response -> {
                    System.out.println("Compressed CSV ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestionOperation(response, "Compressed CSV Stream");
                })
                .whenComplete((unused, throwable) -> closeQuietly(compressedCsvStream));
        futures.add(compressedFuture);

        // Example 3: Ingest JSON with mapping
        InputStream jsonStream = new ByteArrayInputStream(readResourceBytes(resourcesDirectory, "dataset.json"));

        StreamSource jsonStreamSource = new StreamSource(
                jsonStream,
                Format.json, CompressionType.NONE,
                UUID.randomUUID(),
                "json-queued-stream",
                false
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing JSON file with mapping...");
        CompletableFuture<Void> jsonFuture = queuedIngestClient.ingestAsync(jsonStreamSource, jsonProperties)
                .thenCompose(response -> {
                    System.out.println("JSON ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestionOperation(response, "JSON Stream");
                })
                .whenComplete((unused, throwable) -> closeQuietly(jsonStream));
        futures.add(jsonFuture);

        return futures;
    }

    /**
     * Demonstrates ingestion from file sources including:
     * - CSV file
     * - Compressed JSON file with mapping
     */
    static List<CompletableFuture<Void>> ingestFromFile() {
        System.out.println("\n=== Queued Ingestion from Files ===");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

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
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing CSV file...");
        CompletableFuture<Void> csvFuture = queuedIngestClient.ingestAsync(csvFileSource, csvProperties)
                .thenCompose(response -> {
                    System.out.println("CSV file ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestionOperation(response, "CSV File");
                });
        futures.add(csvFuture);

        // Example 2: Ingest compressed JSON file with mapping
        FileSource jsonFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP
        );

        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withIngestionMappingReference(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing compressed JSON file with mapping...");
        CompletableFuture<Void> jsonFuture = queuedIngestClient.ingestAsync(jsonFileSource, jsonProperties)
                .thenCompose(response -> {
                    System.out.println("Compressed JSON file ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestionOperation(response, "Compressed JSON File");
                });
        futures.add(jsonFuture);

        return futures;
    }

    /**
     * Demonstrates batch ingestion from multiple sources in a single operation.
     * This is more efficient than ingesting sources one by one when you have multiple files.
     */
    static CompletableFuture<Void> ingestMultipleSources() {
        System.out.println("\n=== Queued Ingestion from Multiple Sources (Batch) ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Create multiple file sources
        FileSource source1 = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.NONE
        );

        FileSource source2 = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv.gz"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.GZIP
        );

        List<IngestionSource> sources = Arrays.asList(source1, source2);

        IngestRequestProperties properties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing multiple sources in batch...");
        return queuedIngestClient.ingestAsync(sources, properties)
                .thenCompose(response -> {
                    System.out.println("Batch ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    System.out.println("Number of sources in batch: " + sources.size());
                    return trackIngestionOperation(response, "Batch Ingestion");
                });
    }

    /**
     * Tracks an ingestion operation by:
     * 1. Getting operation details immediately after queueing
     * 2. Polling for completion
     * 3. Getting final operation details
     * 4. Printing status information
     */
    private static CompletableFuture<Void> trackIngestionOperation(ExtendedIngestResponse response, String operationName) {
        IngestionOperation operation = new IngestionOperation(
                Objects.requireNonNull(response.getIngestResponse().getIngestionOperationId()),
                database,
                table,
                response.getIngestionType()
        );

        System.out.println("\n--- Tracking " + operationName + " ---");

        // Get initial operation details
        return queuedIngestClient.getOperationDetailsAsync(operation)
                .thenCompose(initialDetails -> {
                    System.out.println("[" + operationName + "] Initial Operation Details:");
                    printStatusResponse(initialDetails);

                    // Poll for completion
                    System.out.println("[" + operationName + "] Polling for completion...");
                    return queuedIngestClient.pollForCompletion(operation, Duration.ofSeconds(30),Duration.ofMinutes(2)); // 2 minutes timeout
                })
                .thenCompose(finalStatus -> {
                    System.out.println("[" + operationName + "] Polling completed.");
                    // Get final operation details
                    return queuedIngestClient.getOperationDetailsAsync(operation);
                })
                .thenAccept(finalDetails -> {
                    System.out.println("[" + operationName + "] Final Operation Details:");
                    printStatusResponse(finalDetails);
                    System.out.println("[" + operationName + "] Operation tracking completed.\n");
                })
                .exceptionally(error -> {
                    System.err.println("[" + operationName + "] Error tracking operation: " + error.getMessage());
                    error.printStackTrace();
                    return null;
                });
    }

    /**
     * Prints detailed status information from a StatusResponse
     */
    private static void printStatusResponse(StatusResponse statusResponse) {
        if (statusResponse == null) {
            System.out.println("  Status: null");
            return;
        }

        Status status = statusResponse.getStatus();
        if (status != null) {
            System.out.println("  Summary:");
            System.out.println("    In Progress: " + status.getInProgress());
            System.out.println("    Succeeded: " + status.getSucceeded());
            System.out.println("    Failed: " + status.getFailed());
            System.out.println("    Canceled: " + status.getCanceled());
        }

        List<BlobStatus> details = statusResponse.getDetails();
        if (details != null && !details.isEmpty()) {
            System.out.println("  Blob Details:");
            for (int i = 0; i < details.size(); i++) {
                BlobStatus blobStatus = details.get(i);
                System.out.println("    Blob " + (i + 1) + ":");
                System.out.println("      Source ID: " + blobStatus.getSourceId());
                System.out.println("      Status: " + blobStatus.getStatus());
                if (blobStatus.getDetails() != null) {
                    System.out.println("      Details: " + blobStatus.getDetails());
                }
                if (blobStatus.getErrorCode() != null) {
                    System.out.println("      Error Code: " + blobStatus.getErrorCode());
                }
                if (blobStatus.getFailureStatus() != null) {
                    System.out.println("      Failure Status: " + blobStatus.getFailureStatus());
                }
            }
        }
    }

    private static byte[] readResourceBytes(String baseDirectory, String fileName) throws IOException {
        return Files.readAllBytes(Paths.get(baseDirectory, fileName));
    }

    private static void closeQuietly(InputStream stream) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (Exception e) {
            System.err.println("Failed to close stream: " + e.getMessage());
        }
    }
}
