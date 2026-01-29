// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2;

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These are tests to ensure that Kotlin specific operators and constructs do not break Java compatibility.
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
    @ParameterizedTest(name = "Queued Ingest Basic Test - useIngestRequestProperties={0}")
    @ValueSource(booleans = {true,false})
    public void testBasicQueuedIngest(boolean useIngestRequestProperties) throws Exception {
        logger.info("Running Java queued ingest regression test");

        // Create queued client

        try (QueuedIngestClient client = QueuedIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxConcurrency(10)
                .build()) {
            // Prepare simple JSON data
            String jsonData = "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"deviceId\":\"00000000-0000-0000-0000-000000000001\",\"messageId\":\"00000000-0000-0000-0000-000000000002\",\"temperature\":25.5,\"humidity\":60.0,\"format\":\"json\"}";
            InputStream dataStream = new ByteArrayInputStream(jsonData.getBytes(StandardCharsets.UTF_8));

            StreamSource source = new StreamSource(
                    dataStream,
                    Format.json, CompressionType.NONE,
                    UUID.randomUUID(),
                    false
            );

            IngestionMapping mappingReference = new IngestionMapping(targetTable + "_mapping",
                    IngestionMapping.IngestionMappingType.JSON);

            IngestRequestProperties properties = useIngestRequestProperties ?IngestRequestPropertiesBuilder
                    .create()
                    .withIngestionMapping(mappingReference)
                    .withEnableTracking(true)
                    .build(): null;

            // Queue data for ingestion
            logger.info("Queueing data for ingestion...");
            ExtendedIngestResponse response = client.ingestAsyncJava(database, targetTable,source, properties).get();

            assertNotNull(response, "Response should not be null");
            if(useIngestRequestProperties) {

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
                StatusResponse initialStatus = client.getOperationDetailsAsyncJava(operation).get();
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
                assertNotNull(finalStatus.getStatus(), "Final status summary should not be null");
                assertEquals(0, finalStatus.getStatus().getFailed(), "Ingestion should not record failures");
                assertTrue(finalStatus.getStatus().getSucceeded() != null && finalStatus.getStatus().getSucceeded() >= 1, "At least one ingestion should succeed");
                logger.info("Polling completed");
            }
            // Verify data appeared in table
            String query = String.format("%s | summarize count=count()", targetTable);
            awaitAndQuery(query, 1);

            logger.info("Java queued ingest regression test PASSED");

        }
    }

    /**
     * Test queued ingestion with file source from Java.
     * Verifies that file-based ingestion works correctly.
     */
    @Test
    public void testQueuedIngestFromFileSource() throws Exception {
        logger.info("Running Java queued ingest from file source test");

        try (QueuedIngestClient client = QueuedIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxConcurrency(10)
                .build()) {
            // Use test resource file if available
            String resourcePath = "src/test/resources/compression/sample.multijson";
            java.nio.file.Path filePath = java.nio.file.Paths.get(resourcePath);

            if (!java.nio.file.Files.exists(filePath)) {
                logger.warn("Test file not found: {}, skipping file source test", resourcePath);
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
                    .create()
                    .withEnableTracking(true)
                    .build();

            logger.info("Queueing file for ingestion...");
            ExtendedIngestResponse response = client.ingestAsyncJava(database, targetTable,fileSource, properties).get();

            assertNotNull(response, "Response should not be null");
            logger.info("File queued. Operation ID: {}",
                    response.getIngestResponse().getIngestionOperationId());

            // Track operation
            IngestionOperation operation = new IngestionOperation(
                    Objects.requireNonNull(response.getIngestResponse().getIngestionOperationId()),
                    database,
                    targetTable,
                    response.getIngestionType()
            );

            // Poll for completion
            StatusResponse fileFinalStatus = client.pollForCompletion(
                 operation,
                 Duration.ofSeconds(30),
                 Duration.ofMinutes(2)
            ).get();

            assertNotNull(fileFinalStatus, "File ingestion final status should not be null");
            assertNotNull(fileFinalStatus.getStatus(), "File ingestion summary should not be null");
            assertEquals(0, fileFinalStatus.getStatus().getFailed(), "File ingestion should not record failures");
            assertTrue(fileFinalStatus.getStatus().getSucceeded()!=null && fileFinalStatus.getStatus().getSucceeded() >= 1, "File ingestion should report successes");

             logger.info("Java queued file ingest test PASSED");

         }
     }
 }
