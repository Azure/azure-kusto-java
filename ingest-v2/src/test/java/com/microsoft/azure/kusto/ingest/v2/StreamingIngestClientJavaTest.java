// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2;

import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.StreamingIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Java regression test for StreamingIngestClient.
 * Tests basic streaming ingestion functionality from Java to ensure
 * compatibility with Kotlin-based implementation.
 */
@Execution(ExecutionMode.CONCURRENT)
public class StreamingIngestClientJavaTest extends IngestV2JavaTestBase {

    public StreamingIngestClientJavaTest() {
        super(StreamingIngestClientJavaTest.class);
    }

    /**
     * Test basic streaming ingestion from Java.
     * Verifies that:
     * - Client can be created using builder pattern from Java
     * - Simple CSV data can be ingested via streaming
     * - Data appears in the table after ingestion
     */
    @Test
    public void testBasicStreamingIngest() throws Exception {
        logger.info("Running Java streaming ingest regression test");

        // Enable streaming ingestion on the table
        alterTableToEnableStreaming();

        // Create streaming client

        try (StreamingIngestClient client = StreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("JavaStreamingRegressionTest", "1.0", null)
                .build()) {
            // Prepare simple CSV data
            String jsonData = "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"deviceId\":\"00000000-0000-0000-0000-000000000001\",\"messageId\":\"00000000-0000-0000-0000-000000000002\",\"temperature\":25.5,\"humidity\":60.0,\"format\":\"json\"}";
            InputStream dataStream = new ByteArrayInputStream(jsonData.getBytes(StandardCharsets.UTF_8));

            StreamSource source = new StreamSource(
                    dataStream,
                    Format.json, CompressionType.NONE,
                    UUID.randomUUID(),
                    "java-streaming-test",
                    false
            );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                    .create()
                    .withIngestionMappingReference(targetTable + "_mapping")
                    .build();

            // Ingest data
            logger.info("Ingesting data via streaming...");
            ExtendedIngestResponse response = client.ingestAsync(database, targetTable,source, properties).get();

            assertNotNull(response, "Response should not be null");
            assertNotNull(response.getIngestResponse().getIngestionOperationId(),
                    "Operation ID should not be null");

            logger.info("Streaming ingestion completed. Operation ID: {}",
                    response.getIngestResponse().getIngestionOperationId());

            // Verify data appeared in table
            String query = String.format("%s | summarize count=count()", targetTable);
            awaitAndQuery(query, 1);

            logger.info("Java streaming ingest regression test PASSED");

        }
    }

    /**
     * Test streaming ingestion with compressed data from Java.
     * Verifies that compression handling works correctly from Java.
     */
    @Test
    public void testStreamingIngestWithCompression() throws Exception {
        logger.info("Running Java streaming ingest with compression test");

        alterTableToEnableStreaming();

        try (StreamingIngestClient client = StreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("JavaStreamingCompressionTest", "1.0", null)
                .build()) {
            // Use test resource file
            String resourcePath = "src/test/resources/compression/sample.multijson.gz";
            java.nio.file.Path filePath = java.nio.file.Paths.get(resourcePath);

            if (!java.nio.file.Files.exists(filePath)) {
                logger.warn("Test file not found: {}, skipping compression test", resourcePath);
                return;
            }

            InputStream fileStream = Files.newInputStream(filePath);

            StreamSource source = new StreamSource(
                    fileStream,
                    Format.multijson, CompressionType.GZIP,
                    UUID.randomUUID(),
                    "java-compressed-stream-test",
                    false
            );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                    .create()
                    .build();

            logger.info("Ingesting compressed data...");
            ExtendedIngestResponse response = client.ingestAsync(database, targetTable,source, properties).get();

            assertNotNull(response, "Response should not be null");
            logger.info("Compressed streaming ingestion completed. Operation ID: {}",
                    response.getIngestResponse().getIngestionOperationId());

            fileStream.close();

            logger.info("Java streaming compressed ingest test PASSED");

        } catch (ExecutionException e) {
            logger.error("Ingestion failed", e);
            throw e;
        }
    }
}
