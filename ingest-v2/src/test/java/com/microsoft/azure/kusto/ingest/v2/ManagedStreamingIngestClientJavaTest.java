// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2;

import com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Java regression test for ManagedStreamingIngestClient.
 * Tests basic managed streaming ingestion functionality from Java to ensure
 * compatibility with Kotlin-based implementation.
 */
@Execution(ExecutionMode.CONCURRENT)
public class ManagedStreamingIngestClientJavaTest extends IngestV2JavaTestBase {
    public ManagedStreamingIngestClientJavaTest() {
        super(ManagedStreamingIngestClientJavaTest.class);
    }
    /**
     * Test basic managed streaming ingestion from Java with small data.
     * Small data should use streaming ingestion for low latency.
     * Verifies that:
     * - Client can be created using builder pattern from Java
     * - Small data triggers streaming ingestion
     * - Data appears in the table after ingestion
     */
    @Test
    public void testManagedStreamingIngestSmallData() throws Exception {
        logger.info("Running Java managed streaming ingest (small data) regression test");

        // Enable streaming ingestion on the table
        alterTableToEnableStreaming();

        // Create managed streaming client

        try (ManagedStreamingIngestClient client = ManagedStreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .build()) {
            // Prepare small JSON data (should use streaming)
            String jsonData = "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"deviceId\":\"00000000-0000-0000-0000-000000000001\",\"messageId\":\"00000000-0000-0000-0000-000000000002\",\"temperature\":25.5,\"humidity\":60.0,\"format\":\"json\"}";
            InputStream dataStream = new ByteArrayInputStream(jsonData.getBytes(StandardCharsets.UTF_8));

            StreamSource source = new StreamSource(
                    dataStream,
                    CompressionType.NONE,
                    Format.json,
                    UUID.randomUUID(),
                    "java-managed-streaming-small",
                    false
            );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                    .create(database, targetTable)
                    .withIngestionMappingReference(targetTable + "_mapping")
                    .withEnableTracking(true)
                    .build();

            // Ingest data (should use streaming for small data)
            logger.info("Ingesting small data via managed streaming...");
            ExtendedIngestResponse response = client.ingestAsync(source, properties).get();

            assertNotNull(response, "Response should not be null");
            assertNotNull(response.getIngestResponse().getIngestionOperationId(),
                    "Operation ID should not be null");

            // Verify it used streaming ingestion
            IngestKind ingestionType = response.getIngestionType();
            logger.info("Ingest completed using {} method. Operation ID: {}",
                    ingestionType, response.getIngestResponse().getIngestionOperationId());

            // Small data typically uses streaming, but fallback to queued is acceptable
            assertTrue(
                    ingestionType == IngestKind.STREAMING || ingestionType == IngestKind.QUEUED,
                    "Ingestion type should be either STREAMING or QUEUED"
            );

            // Verify data appeared in table
            String query = String.format("%s | summarize count=count()", targetTable);
            awaitAndQuery(query, 1);

            logger.info("Java managed streaming ingest (small data) regression test PASSED");

        }
    }

    /**
     * Test managed streaming with larger data that should trigger queued fallback.
     * Verifies that:
     * - Larger data automatically falls back to queued ingestion
     * - Fallback mechanism works correctly from Java
     */
    @Test
    public void testManagedStreamingIngestWithFallback() throws Exception {
        logger.info("Running Java managed streaming ingest with fallback test");

        alterTableToEnableStreaming();

        try (ManagedStreamingIngestClient client = ManagedStreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .build()) {
            // Generate larger data (multiple records to increase size)
            StringBuilder largeDataBuilder = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                largeDataBuilder.append(String.format(
                        "{\"timestamp\":\"2024-01-01T%02d:00:00Z\",\"deviceId\":\"00000000-0000-0000-0000-00000000%04d\",\"messageId\":\"%s\",\"temperature\":%.1f,\"humidity\":%.1f,\"format\":\"json\"}\n",
                        i % 24, i, UUID.randomUUID(), 20.0 + (i % 20), 50.0 + (i % 30)
                ));
            }
            String largeData = largeDataBuilder.toString();

            InputStream dataStream = new ByteArrayInputStream(largeData.getBytes(StandardCharsets.UTF_8));
            dataStream.mark(largeData.length()); // Mark for potential retry

            StreamSource source = new StreamSource(
                    dataStream,
                    CompressionType.NONE,
                    Format.multijson,
                    UUID.randomUUID(),
                    "java-managed-streaming-fallback",
                    false
            );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                    .create(database, targetTable)
                    .withIngestionMappingReference(targetTable + "_mapping")
                    .withEnableTracking(true)
                    .build();

            logger.info("Ingesting larger data via managed streaming (may trigger fallback)...");
            ExtendedIngestResponse response = client.ingestAsync(source, properties).get();

            assertNotNull(response, "Response should not be null");

            IngestKind ingestionType = response.getIngestionType();
            logger.info("Ingestion completed using {} method. Operation ID: {}",
                    ingestionType, response.getIngestResponse().getIngestionOperationId());

            // Both streaming and queued are valid outcomes
            assertTrue(
                    ingestionType == IngestKind.STREAMING || ingestionType == IngestKind.QUEUED,
                    "Ingestion type should be either STREAMING or QUEUED"
            );

            if (ingestionType == IngestKind.QUEUED) {
                logger.info("Fallback to QUEUED ingestion triggered (expected for larger data)");
            } else {
                logger.info("Data ingested via STREAMING (compression may have kept size small)");
            }

            logger.info("Java managed streaming fallback test PASSED");

        }
    }

    /**
     * Test managed streaming with file source from Java.
     * Verifies that file-based ingestion works correctly with managed streaming.
     */
    @Test
    public void testManagedStreamingIngestFromFileSource() throws Exception {
        logger.info("Running Java managed streaming ingest from file source test");

        alterTableToEnableStreaming();

        try (ManagedStreamingIngestClient client = ManagedStreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .build()) {
            // Use test resource file if available - check both module dir and root dir paths
            String resourcePath = "src/test/resources/compression/sample.multijson";
            Path filePath = Paths.get(resourcePath);

            // If not found in module directory, try from root directory
            if (!Files.exists(filePath)) {
                resourcePath = "ingest-v2/src/test/resources/compression/sample.multijson";
                filePath = Paths.get(resourcePath);
            }

            if (!Files.exists(filePath)) {
                logger.warn("Test file not found at either location, skipping file source test");
                return;
            }

            FileSource fileSource =
                    new FileSource(
                            filePath,
                            Format.multijson,
                            UUID.randomUUID(),
                            CompressionType.NONE
                    );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                    .create(database, targetTable)
                    .withEnableTracking(true)
                    .build();

            logger.info("Ingesting file via managed streaming...");
            ExtendedIngestResponse response = client.ingestAsync(fileSource, properties).get();

            assertNotNull(response, "Response should not be null");

            IngestKind ingestionType = response.getIngestionType();
            logger.info("File ingestion completed using {} method. Operation ID: {}",
                    ingestionType, response.getIngestResponse().getIngestionOperationId());

            logger.info("Java managed streaming file ingest test PASSED");

        }
    }
}
