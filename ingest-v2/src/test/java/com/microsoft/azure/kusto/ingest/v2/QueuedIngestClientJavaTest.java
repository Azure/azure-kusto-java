// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2;

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Java regression test for QueuedIngestClient.
 * Tests basic queued ingestion functionality from Java to ensure
 * compatibility with Kotlin-based implementation.
 */
@Execution(ExecutionMode.CONCURRENT)
public class QueuedIngestClientJavaTest extends IngestV2JavaTestBase {

    public QueuedIngestClientJavaTest() {
        super(QueuedIngestClientJavaTest.class);
    }

    /**
     * Test basic queued ingestion from Java.
     * Verifies that:
     * - Client can be created using builder pattern from Java
     * - Simple data can be queued for ingestion
     * - Operation can be tracked
     * - Data appears in the table after processing
     */
    @Test
    public void testBasicQueuedIngestFromJava() throws Exception {
        logger.info("Running Java queued ingest regression test");

        // Create queued client
        QueuedIngestClient client = QueuedIngestClientBuilder.create(engineEndpoint)
            .withAuthentication(tokenProvider)
            .withMaxConcurrency(10)
            .build();

        try {
            // Prepare simple JSON data
            String jsonData = "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"deviceId\":\"00000000-0000-0000-0000-000000000001\",\"messageId\":\"00000000-0000-0000-0000-000000000002\",\"temperature\":25.5,\"humidity\":60.0,\"format\":\"json\"}";
            InputStream dataStream = new ByteArrayInputStream(jsonData.getBytes(StandardCharsets.UTF_8));

            StreamSource source = new StreamSource(
                dataStream,
                CompressionType.NONE,
                Format.json,
                UUID.randomUUID(),
                "java-queued-test",
                false
            );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                .create(database, targetTable)
                .withIngestionMappingReference(targetTable + "_mapping")
                .withEnableTracking(true)
                .build();

            // Queue data for ingestion
            logger.info("Queueing data for ingestion...");
            ExtendedIngestResponse response = client.ingestAsync(source, properties).get();

            assertNotNull(response, "Response should not be null");
            assertNotNull(response.getIngestResponse().getIngestionOperationId(), 
                "Operation ID should not be null");
            
            logger.info("Data queued. Operation ID: {}", 
                response.getIngestResponse().getIngestionOperationId());

            // Track the operation
            IngestionOperation operation = new IngestionOperation(
                response.getIngestResponse().getIngestionOperationId(),
                database,
                targetTable,
                response.getIngestionType()
            );

            // Get initial status
            StatusResponse initialStatus = client.getOperationDetailsAsync(operation).get();
            assertNotNull(initialStatus, "Initial status should not be null");
            logger.info("Initial status retrieved");

            // Poll for completion
            logger.info("Polling for completion...");
            StatusResponse finalStatus = client.pollForCompletion(
                operation,
                Duration.ofSeconds(30),
                Duration.ofMinutes(2)
            ).get();

            assertNotNull(finalStatus, "Final status should not be null");
            logger.info("Polling completed");

            // Verify data appeared in table
            String query = String.format("%s | summarize count=count()", targetTable);
            awaitAndQuery(query, 1);

            logger.info("Java queued ingest regression test PASSED");

        } finally {
            client.close();
        }
    }

    /**
     * Test queued ingestion with file source from Java.
     * Verifies that file-based ingestion works correctly.
     */
    @Test
    public void testQueuedIngestFromFileSourceJava() throws Exception {
        logger.info("Running Java queued ingest from file source test");

        QueuedIngestClient client = QueuedIngestClientBuilder.create(engineEndpoint)
            .withAuthentication(tokenProvider)
            .withMaxConcurrency(10)
            .build();

        try {
            // Use test resource file if available
            String resourcePath = "src/test/resources/compression/sample.multijson";
            java.nio.file.Path filePath = java.nio.file.Paths.get(resourcePath);
            
            if (!java.nio.file.Files.exists(filePath)) {
                logger.warn("Test file not found: {}, skipping file source test", resourcePath);
                return;
            }

            com.microsoft.azure.kusto.ingest.v2.source.FileSource fileSource = 
                new com.microsoft.azure.kusto.ingest.v2.source.FileSource(
                    filePath,
                    Format.multijson,
                    UUID.randomUUID(),
                    CompressionType.NONE
                );

            IngestRequestProperties properties = IngestRequestPropertiesBuilder
                .create(database, targetTable)
                .withEnableTracking(true)
                .build();

            logger.info("Queueing file for ingestion...");
            ExtendedIngestResponse response = client.ingestAsync(fileSource, properties).get();

            assertNotNull(response, "Response should not be null");
            logger.info("File queued. Operation ID: {}", 
                response.getIngestResponse().getIngestionOperationId());

            // Track operation
            IngestionOperation operation = new IngestionOperation(
                response.getIngestResponse().getIngestionOperationId(),
                database,
                targetTable,
                response.getIngestionType()
            );

            // Poll for completion
            client.pollForCompletion(
                operation,
                Duration.ofSeconds(30),
                Duration.ofMinutes(2)
            ).get();

            logger.info("Java queued file ingest test PASSED");

        } finally {
            client.close();
        }
    }
}
