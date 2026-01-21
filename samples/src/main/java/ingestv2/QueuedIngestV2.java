// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
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
 * Sample demonstrating queued ingestion using the new ingest-v2 API. This is the modern API that
 * uses Kotlin-based clients with coroutines, providing better async support and a cleaner API
 * design. Queued ingestion is asynchronous and provides reliable, high-throughput data ingestion
 * with operation tracking capabilities.
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

            // Collect all futures for non-blocking execution
            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            // Run ingestion examples
            allFutures.addAll(ingestFromStream());
            allFutures.addAll(ingestFromFile());
            allFutures.add(ingestMultipleSources());

            // Wait for all operations to complete
            CompletableFuture<Void> allOf = CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));

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
     * Demonstrates ingestion from various stream sources including: - In-memory string data as CSV
     * - Compressed file stream (CSV) - JSON file stream with mapping
     *
     * <p>Shows both source configuration with defaults) and source configuration with full control) approaches.
     */
    static List<CompletableFuture<Void>> ingestFromStream() throws Exception {
        System.out.println("\n=== Queued Ingestion from Streams ===");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Example 1: Ingest from in-memory CSV string (only 2 required parameters)
        // sourceCompression defaults to CompressionType.NONE, sourceId auto-generated, baseName null, leaveOpen false
        String csvData =
                "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream =
                new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(csvInputStream, Format.csv);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        System.out.println("Queueing CSV data from string...");
        CompletableFuture<Void> csvFuture =
                queuedIngestClient
                        .ingestAsync(database, table, csvStreamSource, csvProperties)
                        .thenCompose(
                                response -> {
                                    System.out.println(
                                            "CSV ingestion queued. Operation ID: "
                                                    + response.getIngestResponse()
                                                            .getIngestionOperationId());
                                    return trackIngestionOperation(response, "CSV Stream");
                                })
                        .whenComplete((unused, throwable) -> closeQuietly(csvInputStream));
        futures.add(csvFuture);

        // Example 2: Ingest from compressed CSV file (all 6 parameters needed)
        // Explicitly specify compression, sourceId, baseName, and leaveOpen
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        InputStream compressedCsvStream = new ByteArrayInputStream(readResourceBytes(resourcesDirectory, "dataset.csv.gz"));

        StreamSource compressedStreamSource =
                new StreamSource(
                        compressedCsvStream,
                        Format.csv,
                        CompressionType.GZIP,
                        UUID.randomUUID(),
                        false);

        System.out.println("Queueing compressed CSV file...");
        CompletableFuture<Void> compressedFuture =
                queuedIngestClient
                        .ingestAsync(database, table, compressedStreamSource, csvProperties)
                        .thenCompose(
                                response -> {
                                    System.out.println(
                                            "Compressed CSV ingestion queued. Operation ID: "
                                                    + response.getIngestResponse()
                                                            .getIngestionOperationId());
                                    return trackIngestionOperation(
                                            response, "Compressed CSV Stream");
                                })
                        .whenComplete((unused, throwable) -> closeQuietly(compressedCsvStream));
        futures.add(compressedFuture);

        // Example 3: Ingest JSON with mapping - with defaults
        // Uses defaults: sourceCompression=NONE, auto-generated sourceId, leaveOpen=false
        InputStream jsonStream =
                new ByteArrayInputStream(readResourceBytes(resourcesDirectory, "dataset.json"));

        StreamSource jsonStreamSource = new StreamSource(jsonStream, Format.json);

        IngestionMapping mapping = new IngestionMapping(mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties =
                IngestRequestPropertiesBuilder.create()
                        .withIngestionMapping(mapping)
                        .withEnableTracking(true)
                        .build();

        System.out.println("Queueing JSON file with mapping...");
        CompletableFuture<Void> jsonFuture =
                queuedIngestClient
                        .ingestAsync(database, table, jsonStreamSource, jsonProperties)
                        .thenCompose(
                                response -> {
                                    System.out.println(
                                            "JSON ingestion queued. Operation ID: "
                                                    + response.getIngestResponse()
                                                            .getIngestionOperationId());
                                    return trackIngestionOperation(response, "JSON Stream");
                                })
                        .whenComplete((unused, throwable) -> closeQuietly(jsonStream));
        futures.add(jsonFuture);

        return futures;
    }

    /**
     * Demonstrates ingestion from file sources including: - CSV file - Compressed JSON file with
     * mapping
     *
     * Shows both source configuration with defaults and source configuration with all params approaches.
     */
    static List<CompletableFuture<Void>> ingestFromFile() {
        System.out.println("\n=== Queued Ingestion from Files ===");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest CSV file - with defaults
        // compressionType auto-detected from filename (.csv = NONE), sourceId auto-generated, baseName auto-extracted
        FileSource csvFileSource = new FileSource(Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);

        IngestRequestProperties csvProperties =
                IngestRequestPropertiesBuilder.create()
                        .withEnableTracking(true)
                        .build();

        System.out.println("Queueing CSV file...");
        CompletableFuture<Void> csvFuture =
                queuedIngestClient
                        .ingestAsync(database, table, csvFileSource, csvProperties)
                        .thenCompose(
                                response -> {
                                    System.out.println(
                                            "CSV file ingestion queued. Operation ID: "
                                                    + response.getIngestResponse()
                                                            .getIngestionOperationId());
                                    return trackIngestionOperation(response, "CSV File");
                                });
        futures.add(csvFuture);

        // Example 2: Ingest compressed JSON file with mapping - with all parameters specified
        // Explicitly specify sourceId, compression (auto-detected from .gz), and baseName for full control
        FileSource jsonFileSource =
                new FileSource(
                        Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                        Format.json,
                        UUID.randomUUID(),
                        CompressionType.GZIP);

        IngestionMapping mapping = new IngestionMapping(mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties =
                IngestRequestPropertiesBuilder.create()
                        .withIngestionMapping(mapping)
                        .withEnableTracking(true)
                        .build();

        System.out.println("Queueing compressed JSON file with mapping...");
        CompletableFuture<Void> jsonFuture =
                queuedIngestClient
                        .ingestAsync(database, table, jsonFileSource, jsonProperties)
                        .thenCompose(
                                response -> {
                                    System.out.println(
                                            "Compressed JSON file ingestion queued. Operation ID: "
                                                    + response.getIngestResponse()
                                                            .getIngestionOperationId());
                                    return trackIngestionOperation(
                                            response, "Compressed JSON File");
                                });
        futures.add(jsonFuture);

        return futures;
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
     * <p>This example uses public blob URLs from the Kusto sample files to demonstrate
     * multi-blob batch ingestion. All blobs must have the same format.
     */
    static CompletableFuture<Void> ingestMultipleSources() {
        System.out.println("\n=== Queued Ingestion from Multiple Blob Sources (Batch) ===");

        // Multi-source API only accepts BlobSource - not FileSource or StreamSource.
        // If you have local files, you must upload them to blob storage first.
        // Here we use public sample blob URLs from Kusto sample files to demonstrate the pattern.

        // IMPORTANT: All sources in a batch must have the same format!
        // BlobSource constructor requires: blobPath, format, sourceId, compressionType, baseName

        // Using multiple JSON files from Kusto public sample files
        // All files are JSON format - this is required for batch ingestion
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

        // Create list with all blob sources - all must have identical format
        List<BlobSource> blobSources = Arrays.asList(blob1, blob2);
        IngestionMapping mapping = new IngestionMapping(mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties properties =
                IngestRequestPropertiesBuilder.create()
                        .withIngestionMapping(mapping)
                        .withEnableTracking(true)
                        .build();

        System.out.println("Queueing " + blobSources.size() + " blob sources in batch...");
        for (int i = 0; i < blobSources.size(); i++) {
            System.out.println("  Blob " + (i + 1) + ": " + blobSources.get(i).getName());
        }

        return queuedIngestClient
                .ingestAsync(database, table, blobSources, properties)
                .thenCompose(response -> {
                    System.out.println(
                            "Batch ingestion queued. Operation ID: "
                                    + response.getIngestResponse().getIngestionOperationId());
                    System.out.println("Number of sources in batch: " + blobSources.size());
                    return trackIngestionOperation(response, "Batch Blob Ingestion");
                });
    }

    /**
     * Demonstrates ingesting multiple local files by uploading them to blob storage first,
     * then ingesting them as a batch using the multi-source ingestAsync API.
     *
     * <p>Pattern:
     * <ol>
     *   <li>Create a ManagedUploader with proper configuration</li>
     *   <li>Create list of LocalSource (FileSource) objects</li>
     *   <li>Call uploader.uploadManyAsyncJava(localSources) to upload all files to blob storage</li>
     *   <li>Convert successful upload results to BlobSource list</li>
     *   <li>Call queuedIngestClient.ingestAsync(blobSources, properties) to ingest as a batch</li>
     * </ol>
     *
     * <p>This approach allows batch ingestion of local files by first uploading them
     * to blob storage, which is required because the multi-source API only accepts BlobSource.
     */
    static CompletableFuture<Void> ingestMultipleLocalFilesViaBlobUpload(
            String engineEndpoint, ChainedTokenCredential credential) {
        System.out.println("\n=== Queued Ingestion: Upload Local Files to Blob, Then Ingest ===");

        // Step 1: Create configuration cache (needed for ManagedUploader)
        String dmUrl = engineEndpoint.replace(".kusto.", ".ingest-");

        ConfigurationCache configCache =
                DefaultConfigurationCache.create(
                        dmUrl,
                        credential,
                        new ClientDetails("QueuedIngestV2Sample", "1.0", "ingest-v2-sample"));

        // Step 2: Create ManagedUploader for batch uploading local files to blob storage
        ManagedUploader uploader =
                ManagedUploader.builder()
                        .withConfigurationCache(configCache)
                        .withRetryPolicy(new SimpleRetryPolicy())
                        .withMaxConcurrency(10)
                        .withMaxDataSize(4L * 1024 * 1024 * 1024) // 4GB max size
                        .withUploadMethod(UploadMethod.STORAGE)
                        .withTokenCredential(credential)
                        .build();

        System.out.println("ManagedUploader created for batch upload");

        // Step 3: Prepare list of local files to upload (all same format - CSV)
        String resourcesDirectory = System.getProperty("user.dir") + "/src/main/resources/";

        // IMPORTANT: All files must have the same format for batch ingestion!
        FileSource file1 = new FileSource(Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);
        FileSource file2 = new FileSource(Paths.get(resourcesDirectory + "dataset.csv.gz"), Format.csv);

        List<LocalSource> localSources = Arrays.asList(file1, file2);

        System.out.println("Prepared " + localSources.size() + " local files for upload:");
        for (LocalSource source : localSources) {
            System.out.println("  - " + source.getName() + " (format: " + source.getFormat() + ")");
        }

        IngestRequestProperties properties =
                IngestRequestPropertiesBuilder.create()
                        .withEnableTracking(true)
                        .build();

        // Step 4: Upload all local files to blob storage using uploadManyAsync
        // Note: The Kotlin suspend function uploadManyAsync is exposed to Java as uploadManyAsync
        // (via @JvmName annotation) and returns CompletableFuture<UploadResults>
        System.out.println("Uploading " + localSources.size() + " files to blob storage...");

        return uploader.uploadManyAsync(localSources)
                .thenCompose(uploadResults -> {
                    // Step 5: Process upload results
                    System.out.println("Upload completed:");
                    System.out.println("  Successes: " + uploadResults.getSuccesses().size());
                    System.out.println("  Failures: " + uploadResults.getFailures().size());

                    // Log any failures
                    for (var failure : uploadResults.getFailures()) {
                        System.err.println("  Upload failed for " + failure.getSourceName()
                                + ": " + failure.getErrorMessage());
                    }

                    // Step 6: Convert successful uploads to BlobSource list
                    List<BlobSource> blobSources = new ArrayList<>();
                    for (var success : uploadResults.getSuccesses()) {
                        System.out.println("  Uploaded: " + success.getSourceName()
                                + " -> " + success.getBlobUrl().split("\\?")[0]); // Hide SAS token in log

                        // Create BlobSource from upload result
                        // Match format from original FileSource (CSV in this case)
                        BlobSource blobSource = new BlobSource(
                                success.getBlobUrl(),
                                Format.csv,  // All our files are CSV format
                                UUID.randomUUID(),
                                CompressionType.GZIP  // Uploader auto-compresses to GZIP
                        );
                        blobSources.add(blobSource);
                    }

                    if (blobSources.isEmpty()) {
                        return CompletableFuture.<Void>failedFuture(
                                new RuntimeException("All uploads failed - nothing to ingest"));
                    }

                    // Step 7: Ingest all blobs as a batch
                    System.out.println("Ingesting " + blobSources.size() + " blobs as a batch...");
                    return queuedIngestClient.ingestAsync(database, table, blobSources, properties)
                            .thenCompose(response -> {
                                System.out.println(
                                        "Batch ingestion queued. Operation ID: "
                                                + response.getIngestResponse().getIngestionOperationId());
                                System.out.println("Number of sources in batch: " + blobSources.size());
                                return trackIngestionOperation(response, "Local Files Via Blob Upload");
                            });
                })
                .whenComplete((unused, throwable) -> {
                    // Clean up uploader
                    uploader.close();
                    System.out.println("ManagedUploader closed");
                });
    }

    /**
     * Tracks an ingestion operation by: 1. Getting operation details immediately after queueing 2.
     * Polling for completion 3. Getting final operation details 4. Printing status information
     */
    private static CompletableFuture<Void> trackIngestionOperation(
            ExtendedIngestResponse response, String operationName) {
        IngestionOperation operation = new IngestionOperation(
                Objects.requireNonNull(
                        response.getIngestResponse().getIngestionOperationId()),
                database,
                table,
                response.getIngestionType());

        System.out.println("\n--- Tracking " + operationName + " ---");

        // Get initial operation details
        return queuedIngestClient
                .getOperationDetailsAsync(operation)
                .thenCompose(
                        initialDetails -> {
                            System.out.println(
                                    "[" + operationName + "] Initial Operation Details:");
                            printStatusResponse(initialDetails);

                            // Poll for completion
                            System.out.println("[" + operationName + "] Polling for completion...");
                            return queuedIngestClient.pollForCompletion(
                                    operation,
                                    Duration.ofSeconds(30),
                                    Duration.ofMinutes(2)); // 2 minutes timeout
                        })
                .thenCompose(
                        finalStatus -> {
                            System.out.println("[" + operationName + "] Polling completed.");
                            // Get final operation details
                            return queuedIngestClient.getOperationDetailsAsync(operation);
                        })
                .thenAccept(
                        finalDetails -> {
                            System.out.println("[" + operationName + "] Final Operation Details:");
                            printStatusResponse(finalDetails);
                            System.out.println(
                                    "[" + operationName + "] Operation tracking completed.\n");
                        })
                .exceptionally(
                        error -> {
                            System.err.println(
                                    "["
                                            + operationName
                                            + "] Error tracking operation: "
                                            + error.getMessage());
                            error.printStackTrace();
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
