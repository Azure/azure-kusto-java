// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.IngestClientBase;
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache;
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy;
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.Status;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource;
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping;
import com.microsoft.azure.kusto.ingest.v2.models.*;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader;
import com.microsoft.azure.kusto.ingest.v2.uploader.ManagedUploader;
import com.microsoft.azure.kusto.ingest.v2.uploader.UploadMethod;
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache;

import org.jetbrains.annotations.NotNull;

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
 * 
 * <p>This sample shows simple, blocking examples that are easy to understand.
 * For production use, consider using the async API with CompletableFuture to avoid
 * blocking threads - see {@link #advancedAsyncIngestionExample()} for an example.
 * 
 * <p>Queued ingestion is asynchronous on the server side and provides reliable, 
 * high-throughput data ingestion with operation tracking capabilities.
 */
public class QueuedIngestV2 {

    private static String database;
    private static String table;
    private static String mappingName;
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
            mappingName = System.getProperty("dataMappingName");

            TokenCredential credential;

            // Create Azure AD credential
            if (StringUtils.isNotBlank(appId)
                    && StringUtils.isNotBlank(appKey)
                    && StringUtils.isNotBlank(tenant)) {
                credential = new ClientSecretCredentialBuilder()
                        .clientId(appId)
                        .clientSecret(appKey)
                        .tenantId(tenant)
                        .build();
            } else {
                // If there is no app credentials were passed, use AzCli be used for auth
                credential = new AzureCliCredentialBuilder().build();
            }

            if (engineEndpoint == null || engineEndpoint.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cluster endpoint (clusterPath) must be provided as a system property.");
            }

            // Create queued ingest client using the new v2 API
            queuedIngestClient = QueuedIngestClientBuilder.create(engineEndpoint)
                    .withAuthentication(credential)
                    .withMaxConcurrency(10) // Set maximum concurrent uploads
                    .build();

            System.out.println("Queued Ingest Client created successfully");

            // Run simple blocking ingestion examples
            ingestFromStream();
            ingestFromFile();
            ingestMultipleSources();

            // Uncomment to see the advanced async pattern:
            // advancedAsyncIngestionExample();

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
     *
     * <p>These examples use blocking .get() calls for simplicity. In production,
     * consider using the async API with CompletableFuture composition instead.
     */
    static void ingestFromStream() throws Exception {
        System.out.println("\n=== Queued Ingestion from Streams ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest from in-memory CSV string
        String csvData =
                "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream =
                new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        try {
            StreamSource csvStreamSource = new StreamSource(csvInputStream, Format.csv);

            IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                    .withEnableTracking(true)
                    .build();

            System.out.println("Ingesting CSV data from string...");
            // Note: ingestAsync returns a CompletableFuture. We call .get() to block and wait for completion.
            // In production, you might compose futures instead of blocking - see advancedAsyncIngestionExample().
            ExtendedIngestResponse response = queuedIngestClient
                    .ingestAsyncJava(database, table, csvStreamSource, csvProperties)
                    .get(2, TimeUnit.MINUTES);

            System.out.println("CSV ingestion queued successfully. Operation ID: "
                    + response.getIngestResponse().getIngestionOperationId());
            trackIngestionOperation(response, "CSV Stream");
        } finally {
            closeQuietly(csvInputStream);
        }

        // Example 2: Ingest from compressed CSV file
        InputStream compressedCsvStream = new ByteArrayInputStream(
                readResourceBytes(resourcesDirectory, "dataset.csv.gz"));

        try {
            StreamSource compressedStreamSource = new StreamSource(
                    compressedCsvStream,
                    Format.csv,
                    CompressionType.GZIP,
                    UUID.randomUUID(),
                    false);

            IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                    .withEnableTracking(true)
                    .build();

            System.out.println("Ingesting compressed CSV file...");
            ExtendedIngestResponse response = queuedIngestClient
                    .ingestAsyncJava(database, table, compressedStreamSource, csvProperties)
                    .get(2, TimeUnit.MINUTES);

            System.out.println("Compressed CSV ingestion queued successfully. Operation ID: "
                    + response.getIngestResponse().getIngestionOperationId());
            trackIngestionOperation(response, "Compressed CSV Stream");
        } finally {
            closeQuietly(compressedCsvStream);
        }

        // Example 3: Ingest JSON with mapping
        InputStream jsonStream = new ByteArrayInputStream(
                readResourceBytes(resourcesDirectory, "dataset.json"));

        try {
            StreamSource jsonStreamSource = new StreamSource(jsonStream, Format.json);

            IngestionMapping mapping = new IngestionMapping(
                    mappingName, IngestionMapping.IngestionMappingType.JSON);
            IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                    .withIngestionMapping(mapping)
                    .withEnableTracking(true)
                    .build();

            System.out.println("Ingesting JSON file with mapping...");
            ExtendedIngestResponse response = queuedIngestClient
                    .ingestAsyncJava(database, table, jsonStreamSource, jsonProperties)
                    .get(2, TimeUnit.MINUTES);

            System.out.println("JSON ingestion queued successfully. Operation ID: "
                    + response.getIngestResponse().getIngestionOperationId());
            trackIngestionOperation(response, "JSON Stream");
        } finally {
            closeQuietly(jsonStream);
        }
    }

    /**
     * Demonstrates ingestion from file sources including:
     * - CSV file
     * - Compressed JSON file with mapping
     *
     * <p>These examples use blocking .get() calls for simplicity.
     */
    static void ingestFromFile() throws Exception {
        System.out.println("\n=== Queued Ingestion from Files ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest CSV file
        FileSource csvFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting CSV file...");
        ExtendedIngestResponse csvResponse = queuedIngestClient
                .ingestAsyncJava(database, table, csvFileSource, csvProperties)
                .get(2, TimeUnit.MINUTES);

        System.out.println("CSV file ingestion queued successfully. Operation ID: "
                + csvResponse.getIngestResponse().getIngestionOperationId());
        trackIngestionOperation(csvResponse, "CSV File");

        // Example 2: Ingest compressed JSON file with mapping
        FileSource jsonFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP);

        IngestionMapping mapping = new IngestionMapping(
                mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting compressed JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = queuedIngestClient
                .ingestAsyncJava(database, table, jsonFileSource, jsonProperties)
                .get(2, TimeUnit.MINUTES);

        System.out.println("Compressed JSON file ingestion queued successfully. Operation ID: "
                + jsonResponse.getIngestResponse().getIngestionOperationId());
        trackIngestionOperation(jsonResponse, "Compressed JSON File");
    }

    /**
     * Demonstrates batch ingestion from multiple blob sources in a single operation.
     *
     * <p><b>IMPORTANT:</b> Multi-source ingestion only accepts BlobSource. For local sources
     * (FileSource, StreamSource), you must either:
     * <ol>
     *   <li>Ingest them one by one using the single-source ingestAsync method, or</li>
     *   <li>First upload them to blob storage using uploadManyAsync, then pass the resulting
     *       BlobSource list to ingestAsync</li>
     * </ol>
     *
     * <p>This example uses public blob URLs from the Kusto sample files.
     */
    static void ingestMultipleSources() throws Exception {
        System.out.println("\n=== Queued Ingestion from Multiple Blob Sources (Batch) ===");

        // IMPORTANT: All sources in a batch must have the same format!
        BlobSource blob1 = new BlobSource(
                "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json",
                Format.json,
                UUID.randomUUID(),
                CompressionType.NONE);

        BlobSource blob2 = new BlobSource(
                "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/array.json",
                Format.json,
                UUID.randomUUID(),
                CompressionType.NONE);

        List<BlobSource> blobSources = Arrays.asList(blob1, blob2);

        IngestionMapping mapping = new IngestionMapping(
                mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties properties = IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting " + blobSources.size() + " blob sources in batch...");
        for (int i = 0; i < blobSources.size(); i++) {
            System.out.println("  Blob " + (i + 1) + ": " + blobSources.get(i).getName());
        }

        ExtendedIngestResponse response = queuedIngestClient
                .ingestAsyncJava(database, table, blobSources, properties)
                .get(2, TimeUnit.MINUTES);

        System.out.println("Batch ingestion queued successfully. Operation ID: "
                + response.getIngestResponse().getIngestionOperationId());
        System.out.println("Number of sources in batch: " + blobSources.size());
        trackIngestionOperation(response, "Batch Blob Ingestion");
    }

    /**
     * Advanced example demonstrating non-blocking async ingestion using CompletableFuture.
     * 
     * <p>This pattern is recommended for production use when you need to:
     * <ul>
     *   <li>Ingest multiple sources concurrently without blocking threads</li>
     *   <li>Compose multiple async operations</li>
     *   <li>Handle results/errors asynchronously</li>
     * </ul>
     * 
     * <p>The key difference from the simple examples is that we compose futures
     * instead of calling .get() immediately, allowing the operations to run concurrently.
     */
    static void advancedAsyncIngestionExample() throws Exception {
        System.out.println("\n=== Advanced Async Ingestion Example ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Create multiple sources to ingest concurrently
        FileSource csvFile = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);
        FileSource jsonFile = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        IngestionMapping mapping = new IngestionMapping(
                mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(mapping)
                .withEnableTracking(true)
                .build();

        // Start both ingestions concurrently - don't call .get() yet!
        System.out.println("Starting concurrent ingestion of CSV and JSON files...");

        CompletableFuture<ExtendedIngestResponse> csvFuture = queuedIngestClient
                .ingestAsyncJava(database, table, csvFile, csvProperties)
                .thenApply(response -> {
                    System.out.println("CSV ingestion queued. Operation ID: "
                            + response.getIngestResponse().getIngestionOperationId());
                    return response;
                });

        CompletableFuture<ExtendedIngestResponse> jsonFuture = queuedIngestClient
                .ingestAsyncJava(database, table, jsonFile, jsonProperties)
                .thenApply(response -> {
                    System.out.println("JSON ingestion queued. Operation ID: "
                            + response.getIngestResponse().getIngestionOperationId());
                    return response;
                });

        // Compose futures to track both operations
        CompletableFuture<Void> csvTracking = csvFuture
                .thenCompose(response -> trackIngestionOperationAsync(response, "CSV File (Async)"));

        CompletableFuture<Void> jsonTracking = jsonFuture
                .thenCompose(response -> trackIngestionOperationAsync(response, "JSON File (Async)"));

        // Wait for all operations to complete
        System.out.println("Waiting for all async operations to complete...");
        CompletableFuture.allOf(csvTracking, jsonTracking).get(5, TimeUnit.MINUTES);

        System.out.println("All async ingestion operations completed!");
    }

    /**
     * Demonstrates ingesting multiple local files by uploading them to blob storage first,
     * then ingesting them as a batch using the multi-source ingestAsync API.
     *
     * <p>Pattern:
     * <ol>
     *   <li>Create a ManagedUploader with proper configuration</li>
     *   <li>Create list of LocalSource (FileSource) objects</li>
     *   <li>Call uploader.uploadManyAsync(localSources) to upload all files to blob storage</li>
     *   <li>Convert successful upload results to BlobSource list</li>
     *   <li>Call queuedIngestClient.ingestAsyncJava(blobSources, properties) to ingest as a batch</li>
     * </ol>
     */
    static void ingestMultipleLocalFilesViaBlobUpload(
            String engineEndpoint, ChainedTokenCredential credential) throws Exception {
        System.out.println("\n=== Queued Ingestion: Upload Local Files to Blob, Then Ingest ===");

        // Step 1: Create configuration cache (needed for ManagedUploader)
        String dmUrl = IngestClientBase.getIngestionEndpoint(engineEndpoint);
        if (dmUrl == null) {
            dmUrl = engineEndpoint;
        }

        ConfigurationCache configCache = DefaultConfigurationCache.create(
                dmUrl,
                credential,
                new ClientDetails("QueuedIngestV2Sample", "1.0", "ingest-v2-sample"));

        // Step 2: Create ManagedUploader
        ManagedUploader uploader = ManagedUploader.builder()
                .withConfigurationCache(configCache)
                .withRetryPolicy(new SimpleRetryPolicy())
                .withMaxConcurrency(10)
                .withMaxDataSize(4L * 1024 * 1024 * 1024) // 4GB max size
                .withUploadMethod(UploadMethod.STORAGE)
                .withTokenCredential(credential)
                .build();

        try {
            System.out.println("ManagedUploader created for batch upload");

            // Step 3: Prepare local files (all same format - CSV)
            String resourcesDirectory = System.getProperty("user.dir") + "/src/main/resources/";
            FileSource file1 = new FileSource(Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);
            FileSource file2 = new FileSource(Paths.get(resourcesDirectory + "dataset.csv.gz"), Format.csv);
            List<LocalSource> localSources = Arrays.asList(file1, file2);

            System.out.println("Prepared " + localSources.size() + " local files for upload");

            // Step 4: Upload all local files to blob storage
            System.out.println("Uploading files to blob storage...");
            var uploadResults = uploader.uploadManyAsync(localSources).get(5, TimeUnit.MINUTES);

            System.out.println("Upload completed:");
            System.out.println("  Successes: " + uploadResults.getSuccesses().size());
            System.out.println("  Failures: " + uploadResults.getFailures().size());

            // Step 5: Convert successful uploads to BlobSource list
            List<BlobSource> blobSources = new ArrayList<>();
            for (var success : uploadResults.getSuccesses()) {
                BlobSource blobSource = new BlobSource(
                        success.getBlobUrl(),
                        Format.csv,
                        UUID.randomUUID(),
                        CompressionType.GZIP);
                blobSources.add(blobSource);
            }

            if (blobSources.isEmpty()) {
                throw new RuntimeException("All uploads failed - nothing to ingest");
            }

            // Step 6: Ingest all blobs as a batch
            IngestRequestProperties properties = IngestRequestPropertiesBuilder.create()
                    .withEnableTracking(true)
                    .build();

            System.out.println("Ingesting " + blobSources.size() + " blobs as a batch...");
            ExtendedIngestResponse response = queuedIngestClient
                    .ingestAsyncJava(database, table, blobSources, properties)
                    .get(2, TimeUnit.MINUTES);

            System.out.println("Batch ingestion queued successfully. Operation ID: "
                    + response.getIngestResponse().getIngestionOperationId());
            trackIngestionOperation(response, "Local Files Via Blob Upload");

        } finally {
            uploader.close();
            System.out.println("ManagedUploader closed");
        }
    }

    /**
     * Tracks an ingestion operation synchronously (blocking).
     */
    private static void trackIngestionOperation(
            ExtendedIngestResponse response, String operationName) throws Exception {
        IngestionOperation operation = new IngestionOperation(
                Objects.requireNonNull(response.getIngestResponse().getIngestionOperationId()),
                database,
                table,
                response.getIngestionType());

        System.out.println("\n--- Tracking " + operationName + " ---");

        // Get initial operation details
        StatusResponse initialDetails = queuedIngestClient
                .getOperationDetailsAsyncJava(operation)
                .get(1, TimeUnit.MINUTES);

        System.out.println("[" + operationName + "] Initial Operation Details:");
        printStatusResponse(initialDetails);

        // Poll for completion
        System.out.println("[" + operationName + "] Polling for completion...");
        queuedIngestClient.pollForCompletion(
                operation,
                Duration.ofSeconds(30),
                Duration.ofMinutes(2))
                .get(3, TimeUnit.MINUTES);

        System.out.println("[" + operationName + "] Polling completed.");

        // Get final operation details
        StatusResponse finalDetails = queuedIngestClient
                .getOperationDetailsAsyncJava(operation)
                .get(1, TimeUnit.MINUTES);

        System.out.println("[" + operationName + "] Final Operation Details:");
        printStatusResponse(finalDetails);
        System.out.println("[" + operationName + "] Operation tracking completed.\n");
    }

    /**
     * Tracks an ingestion operation asynchronously using CompletableFuture composition.
     * Used by {@link #advancedAsyncIngestionExample()}.
     */
    private static CompletableFuture<Void> trackIngestionOperationAsync(
            ExtendedIngestResponse response, String operationName) {
        IngestionOperation operation = new IngestionOperation(
                Objects.requireNonNull(response.getIngestResponse().getIngestionOperationId()),
                database,
                table,
                response.getIngestionType());

        System.out.println("\n--- Tracking " + operationName + " (async) ---");

        return queuedIngestClient
                .getOperationDetailsAsyncJava(operation)
                .thenCompose(initialDetails -> {
                    System.out.println("[" + operationName + "] Initial status received");
                    return queuedIngestClient.pollForCompletion(
                            operation,
                            Duration.ofSeconds(30),
                            Duration.ofMinutes(2));
                })
                .thenCompose(finalStatus -> queuedIngestClient.getOperationDetailsAsyncJava(operation))
                .thenAccept(finalDetails -> {
                    System.out.println("[" + operationName + "] Final Operation Details:");
                    printStatusResponse(finalDetails);
                    System.out.println("[" + operationName + "] Async tracking completed.\n");
                })
                .exceptionally(error -> {
                    System.err.println("[" + operationName + "] Error: " + error.getMessage());
                    return null;
                });
    }

    /** Prints detailed status information from a StatusResponse */
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

    private static byte[] readResourceBytes(String baseDirectory, String fileName)
            throws IOException {
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