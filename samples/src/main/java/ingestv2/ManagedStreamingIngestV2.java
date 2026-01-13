// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping;
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.Status;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Sample demonstrating managed streaming ingestion using the new ingest-v2 API. This is the modern
 * API that uses Kotlin-based clients with coroutines, providing better async support and a cleaner
 * API design. Managed streaming ingestion intelligently chooses between streaming and queued
 * ingestion: - Small data (typically under 4MB) is ingested via streaming for low latency - Large
 * data automatically falls back to queued ingestion for reliability - Server errors (like streaming
 * disabled) trigger automatic fallback to queued - Transient errors are retried according to the
 * configured retry policy This approach provides the best of both worlds: low latency for small
 * data and high reliability for all data sizes.
 */
public class ManagedStreamingIngestV2 {

    private static String database;
    private static String table;
    private static String mappingName;
    private static ManagedStreamingIngestClient managedStreamingIngestClient;

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

            ChainedTokenCredential credential;

            // Create Azure AD credential
            if (StringUtils.isNotBlank(appId)
                    && StringUtils.isNotBlank(appKey)
                    && StringUtils.isNotBlank(tenant)) {
                credential = new ChainedTokenCredentialBuilder()
                        .addFirst(
                                new ClientSecretCredentialBuilder()
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

            if (engineEndpoint == null || engineEndpoint.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cluster endpoint (clusterPath) must be provided as a system property.");
            }

            // Create managed streaming ingest client using the new v2 API
            // The client will automatically handle streaming vs queued ingestion decisions
            managedStreamingIngestClient = ManagedStreamingIngestClientBuilder.create(engineEndpoint)
                    .withAuthentication(credential)
                    .build();

            System.out.println("Managed Streaming Ingest Client created successfully");
            System.out.println(
                    "This client automatically chooses between streaming and queued ingestion");
            System.out.println("based on data size and server responses.\n");

            // Run ingestion examples
            ingestFromStream();
            ingestFromFile();
            demonstrateFallbackTracking();

            System.out.println("\nAll managed streaming ingestion operations completed");

        } catch (Exception e) {
            System.err.println("Error during ingestion: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (managedStreamingIngestClient != null) {
                managedStreamingIngestClient.close();
            }
        }
    }

    /**
     * Demonstrates ingestion from various stream sources. Small data will typically use streaming
     * ingestion for low latency. Sources include: - In-memory string data as CSV (small, will use
     * streaming) - Compressed file stream (CSV) - JSON file stream with mapping
     */
    static void ingestFromStream() throws Exception {
        System.out.println("\n=== Managed Streaming Ingestion from Streams ===");

        // Example 1: Ingest from in-memory CSV string (small data - will use streaming)
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(
                csvInputStream,
                Format.csv,
                CompressionType.NONE,
                UUID.randomUUID(),
                "csv-managed-stream",
                false);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting small CSV data from string...");
        ExtendedIngestResponse csvResponse = managedStreamingIngestClient.ingestAsync(database, table, csvStreamSource, csvProperties).get();
        printIngestionResult("CSV String", csvResponse);

        // Example 2: Ingest from compressed CSV file
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        FileInputStream compressedCsvStream = new FileInputStream(resourcesDirectory + "dataset.csv.gz");

        StreamSource compressedStreamSource = new StreamSource(
                compressedCsvStream,
                Format.csv,
                CompressionType.GZIP,
                UUID.randomUUID(),
                "compressed-csv-managed-stream",
                false);

        System.out.println("Ingesting compressed CSV file...");
        ExtendedIngestResponse compressedResponse = managedStreamingIngestClient
                .ingestAsync(database, table, compressedStreamSource, csvProperties)
                .get();
        printIngestionResult("Compressed CSV", compressedResponse);
        compressedCsvStream.close();

        // Example 3: Ingest JSON with mapping
        FileInputStream jsonStream = new FileInputStream(resourcesDirectory + "dataset.json");

        StreamSource jsonStreamSource = new StreamSource(
                jsonStream,
                Format.json,
                CompressionType.NONE,
                UUID.randomUUID(),
                "json-managed-stream",
                false);
        IngestionMapping ingestionMapping = new IngestionMapping(mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(ingestionMapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = managedStreamingIngestClient.ingestAsync(database, table, jsonStreamSource, jsonProperties).get();
        printIngestionResult("JSON with Mapping", jsonResponse);
        jsonStream.close();
    }

    /**
     * Demonstrates ingestion from file sources. The client will automatically decide between
     * streaming and queued based on file size and other factors. Sources include: - CSV file -
     * Compressed JSON file with mapping
     */
    static void ingestFromFile() throws Exception {
        System.out.println("\n=== Managed Streaming Ingestion from Files ===");

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Example 1: Ingest CSV file
        FileSource csvFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.csv"),
                Format.csv,
                UUID.randomUUID(),
                CompressionType.NONE,
                "m-ds-csv");

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting CSV file...");
        ExtendedIngestResponse csvResponse = managedStreamingIngestClient.ingestAsync(database, table, csvFileSource, csvProperties).get();
        printIngestionResult("CSV File", csvResponse);

        // Example 2: Ingest compressed JSON file with mapping
        FileSource jsonFileSource = new FileSource(
                Paths.get(resourcesDirectory + "dataset.jsonz.gz"),
                Format.json,
                UUID.randomUUID(),
                CompressionType.GZIP,
                "m-ds-json-compressed");

        IngestionMapping ingestionMapping = new IngestionMapping(mappingName, IngestionMapping.IngestionMappingType.JSON);
        IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                .withIngestionMapping(ingestionMapping)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting compressed JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = managedStreamingIngestClient.ingestAsync(database, table, jsonFileSource, jsonProperties).get();
        printIngestionResult("Compressed JSON File", jsonResponse);
    }

    /**
     * Demonstrates the automatic fallback to queued ingestion when data size exceeds the streaming
     * limit. This method creates a large in-memory dataset (~20MB uncompressed) to force the client
     * to fall back to queued ingestion. Note that data is automatically compressed before
     * ingestion, so we use a larger size (20MB) to ensure the compressed data still exceeds the
     * streaming threshold (~4MB compressed). This demonstrates: - Automatic size-based
     * decision-making - Fallback logging from the client ("Blob size is too big for streaming
     * ingest") - Operation tracking for queued ingestion Note: Streaming ingestion operations are
     * not tracked - they complete immediately with success or throw an exception on failure.
     */
    static void demonstrateFallbackTracking() throws Exception {
        System.out.println("\n=== Demonstrating Size-Based Fallback to Queued Ingestion ===");
        System.out.println(
                "The ManagedStreamingIngestClient automatically falls back to queued ingestion");
        System.out.println("when data size exceeds the streaming limit (~4MB compressed).");
        System.out.println(
                "Since data is automatically compressed, we use a larger dataset (~20MB)");
        System.out.println("to ensure the compressed size still exceeds the threshold.\n");

        // Generate a large CSV dataset (~20MB uncompressed) that will exceed the streaming limit
        // even after compression. This will force the client to fall back to queued ingestion.
        int targetSizeBytes = 20 * 1024 * 1024; // 20MB
        String largeData = generateLargeCsvData(targetSizeBytes);
        byte[] dataBytes = largeData.getBytes(StandardCharsets.UTF_8);

        System.out.println("Generated large CSV dataset:");
        System.out.println("  - Uncompressed data size: " + formatBytes(dataBytes.length));
        System.out.println("  - Streaming limit: ~4MB (after compression)");
        System.out.println("  - Expected behavior: FALL BACK TO QUEUED INGESTION");
        System.out.println("  - Look for log message: 'Blob size is too big for streaming ingest'");
        System.out.println();

        InputStream largeInputStream = new ByteArrayInputStream(dataBytes);

        // Mark the stream for potential retry (seekable stream)
        largeInputStream.mark(dataBytes.length);

        StreamSource largeStreamSource = new StreamSource(
                largeInputStream,
                Format.csv,
                CompressionType.NONE, // Will be auto-compressed by the client
                UUID.randomUUID(),
                "large-data-fallback-demo",
                false);

        IngestRequestProperties properties = IngestRequestPropertiesBuilder.create()
                .withEnableTracking(true)
                .build();

        System.out.println(
                "Ingesting large dataset (" + formatBytes(dataBytes.length) + " uncompressed)...");
        System.out.println("(Watch for fallback log messages from ManagedStreamingIngestClient)");
        System.out.println();

        ExtendedIngestResponse response = managedStreamingIngestClient.ingestAsync(database, table, largeStreamSource, properties).get();
        printIngestionResult("Large Data Ingestion", response);

        // The large data should trigger queued fallback
        if (response.getIngestionType() == IngestKind.QUEUED) {
            System.out.println("SUCCESS: Large data correctly triggered QUEUED fallback!");
            System.out.println("This demonstrates the automatic size-based routing.\n");

            IngestionOperation operation = new IngestionOperation(
                    Objects.requireNonNull(
                            response.getIngestResponse().getIngestionOperationId()),
                    database,
                    table,
                    response.getIngestionType());

            // Get initial operation details
            CompletableFuture<StatusResponse> detailsFuture = managedStreamingIngestClient.getOperationDetailsAsync(operation);
            StatusResponse details = detailsFuture.get();
            printStatusResponse("Initial Status", details);

            // Poll for completion using getOperationDetailsAsync
            System.out.println(
                    "\nPolling for completion (checking every 30 seconds, timeout 2 minutes)...");
            StatusResponse finalStatus = pollForCompletionManually(
                    operation, Duration.ofSeconds(30), Duration.ofMinutes(2));
            printStatusResponse("Final Status", finalStatus);

        } else {
            System.out.println("NOTE: Data was ingested via STREAMING method.");
            System.out.println(
                    "This might happen if compression was very effective. Try increasing");
            System.out.println("the data size or using less compressible data patterns.");
        }
    }

    /**
     * Generates a large CSV dataset of approximately the target size. The data follows the format
     * expected by the sample table schema. Uses varied data to reduce compression effectiveness.
     */
    private static @NotNull String generateLargeCsvData(int targetSizeBytes) {
        StringBuilder sb = new StringBuilder();
        int rowCount = 0;

        // Generate varied data to make it less compressible
        java.util.Random random = new java.util.Random(42); // Fixed seed for reproducibility

        // Sample CSV row matching the expected schema
        // Format:
        // int,guid,int,int,int,int,int,int,int,int,int,int,datetime,string,string,int,timespan,null,null
        while (sb.length() < targetSizeBytes) {
            // Use random values to reduce compression effectiveness
            sb.append(rowCount)
                    .append(",")
                    .append(UUID.randomUUID())
                    .append(",") // Random GUID
                    .append(random.nextInt(10000))
                    .append(",")
                    .append(random.nextInt(100000))
                    .append(",")
                    .append(random.nextLong())
                    .append(",")
                    .append(random.nextDouble() * 1000000)
                    .append(",")
                    .append(random.nextInt())
                    .append(",")
                    .append(random.nextInt(1000))
                    .append(",")
                    .append(random.nextInt(5000))
                    .append(",")
                    .append(random.nextInt(10000))
                    .append(",")
                    .append(random.nextInt(100))
                    .append(",")
                    .append(random.nextInt(50))
                    .append(",")
                    .append("2024-")
                    .append(String.format("%02d", (rowCount % 12) + 1))
                    .append("-")
                    .append(String.format("%02d", (rowCount % 28) + 1))
                    .append("T")
                    .append(String.format("%02d", rowCount % 24))
                    .append(":")
                    .append(String.format("%02d", rowCount % 60))
                    .append(":")
                    .append(String.format("%02d", rowCount % 60))
                    .append(".")
                    .append(String.format("%07d", random.nextInt(10000000)))
                    .append("Z")
                    .append(",")
                    .append("Row_")
                    .append(rowCount)
                    .append("_")
                    .append(random.nextInt(100000))
                    .append(",")
                    .append("\"Description with random data: ")
                    .append(random.nextLong())
                    .append(" and more: ")
                    .append(UUID.randomUUID())
                    .append("\"")
                    .append(",")
                    .append(random.nextInt(100000))
                    .append(",")
                    .append(
                            String.format(
                                    "%02d:%02d:%02d",
                                    random.nextInt(24), random.nextInt(60), random.nextInt(60)))
                    .append(",")
                    .append(",")
                    .append("null")
                    .append("\n");
            rowCount++;
        }

        System.out.println("  - Generated " + rowCount + " rows of varied data");
        return sb.toString();
    }

    /** Formats bytes into a human-readable string (e.g., "10.00 MB"). */
    private static @NotNull String formatBytes(long bytes) {
        if (bytes < 1024)
            return bytes + " B";
        if (bytes < 1024 * 1024)
            return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024)
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    /**
     * Manually polls for completion by repeatedly calling getOperationDetailsAsync. This
     * demonstrates how to implement polling when the ManagedStreamingIngestClient is used and
     * queued fallback occurs.
     */
    private static StatusResponse pollForCompletionManually(
            IngestionOperation operation, @NotNull Duration pollingInterval, @NotNull Duration timeout)
            throws Exception {

        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeout.toMillis();
        long intervalMillis = pollingInterval.toMillis();

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            StatusResponse status = managedStreamingIngestClient.getOperationDetailsAsync(operation).get();

            // Check if completed (no more in-progress items)
            Status summary = status.getStatus();
            if (summary != null
                    && summary.getInProgress() != null
                    && summary.getInProgress() == 0) {
                System.out.println("Operation completed.");
                return status;
            }

            System.out.println(
                    "Still in progress... (In Progress: "
                            + (summary != null ? summary.getInProgress() : "unknown")
                            + ")");

            // Wait before next poll
            Thread.sleep(intervalMillis);
        }

        // Timeout reached, return latest status
        System.out.println("Polling timeout reached. Returning latest status.");
        return managedStreamingIngestClient.getOperationDetailsAsync(operation).get();
    }

    /** Prints the ingestion result including which method (streaming or queued) was used. */
    private static void printIngestionResult(
            String operationName, @NotNull ExtendedIngestResponse response) {
        String ingestionMethod = response.getIngestionType() == IngestKind.STREAMING ? "STREAMING" : "QUEUED";
        System.out.println(
                "["
                        + operationName
                        + "] Ingestion completed using "
                        + ingestionMethod
                        + " method.");
        System.out.println(
                "  Operation ID: " + response.getIngestResponse().getIngestionOperationId());
        if (response.getIngestionType() == IngestKind.STREAMING) {
            System.out.println("  (Low latency - data available immediately)");
        } else {
            System.out.println(
                    "  (High reliability - data will be available after batch processing)");
        }
        System.out.println();
    }

    /** Prints detailed status information from a StatusResponse. */
    private static void printStatusResponse(String label, StatusResponse statusResponse) {
        if (statusResponse == null) {
            System.out.println(label + ": null");
            return;
        }

        System.out.println(label + ":");
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
}
