// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestv2;

import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.ManagedStreamingIngestClient;
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
 * Sample demonstrating managed streaming ingestion using the new ingest-v2 API.
 * 
 * Managed streaming combines the benefits of streaming and queued ingestion:
 * - Attempts streaming ingestion first for low latency
 * - Automatically falls back to queued ingestion when:
 *   - Data size exceeds streaming limits (~4MB)
 *   - Streaming ingestion is disabled on the cluster/table
 *   - Transient errors occur after retries are exhausted
 *   - Table configuration prevents streaming
 * 
 * This is the recommended approach for most scenarios as it provides:
 * - Low latency for small data when streaming is available
 * - Reliability through automatic fallback to queued ingestion
 * - Seamless handling of cluster configuration changes
 */
public class ManagedStreamingIngestV2 {

    private static String database;
    private static String table;
    private static String mapping;
    private static ManagedStreamingIngestClient managedStreamingClient;

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

            // Create managed streaming ingest client using the new v2 API
            // This client intelligently chooses between streaming and queued ingestion
            managedStreamingClient = ManagedStreamingIngestClientBuilder.create(ingestionEndpoint)
                    .withAuthentication(credential)
                    .build();

            System.out.println("Managed Streaming Ingest Client created successfully");

            // Run ingestion examples
            ingestFromStream();
            ingestFromFile();

            System.out.println("\nAll ingestion operations completed");

        } catch (Exception e) {
            System.err.println("Error during ingestion: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (managedStreamingClient != null) {
                managedStreamingClient.close();
            }
        }
    }

    /**
     * Demonstrates managed streaming ingestion from various stream sources.
     * 
     * For small data, this will use streaming ingestion.
     * For large data or when streaming fails, it automatically falls back to queued.
     */
    static void ingestFromStream() throws Exception {
        System.out.println("\n=== Managed Streaming Ingestion from Streams ===");

        // Example 1: Ingest from in-memory CSV string (small data -> streaming)
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());

        StreamSource csvStreamSource = new StreamSource(
                csvInputStream, CompressionType.NONE, Format.csv,
                UUID.randomUUID(), "csv-stream-src", false);

        IngestRequestProperties csvProperties = IngestRequestPropertiesBuilder
                .create(database, table)
                .withFormat(Format.csv)
                .withEnableTracking(true)
                .build();

        System.out.println("Ingesting CSV data from string (expecting streaming)...");
        ExtendedIngestResponse ingestResponse = managedStreamingClient.ingestAsync(csvStreamSource, csvProperties).get();
        System.out.println("CSV data ingested. Operation ID: " + ingestResponse.getIngestResponse().getIngestionOperationId());

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

        System.out.println("Ingesting compressed CSV file...");
        ExtendedIngestResponse compressedResponse = managedStreamingClient.ingestAsync(compressedStreamSource, csvProperties).get();
        System.out.println("Compressed CSV ingested. Operation ID: " + compressedResponse.getIngestResponse().getIngestionOperationId());
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

        System.out.println("Ingesting JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = managedStreamingClient.ingestAsync(jsonStreamSource, jsonProperties).get();
        System.out.println("JSON data ingested. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
        jsonStream.close();
    }

    /**
     * Demonstrates managed streaming ingestion from file sources.
     * 
     * The client automatically determines the best ingestion method based on
     * file size, cluster configuration, and streaming availability.
     */
    static void ingestFromFile() throws Exception {
        System.out.println("\n=== Managed Streaming Ingestion from Files ===");

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

        System.out.println("Ingesting CSV file...");
        ExtendedIngestResponse csvResponse = managedStreamingClient.ingestAsync(csvFileSource, csvProperties).get();
        System.out.println("CSV file ingested. Operation ID: " + csvResponse.getIngestResponse().getIngestionOperationId());

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

        System.out.println("Ingesting compressed JSON file with mapping...");
        ExtendedIngestResponse jsonResponse = managedStreamingClient.ingestAsync(jsonFileSource, jsonProperties).get();
        System.out.println("Compressed JSON file ingested. Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
    }
}
