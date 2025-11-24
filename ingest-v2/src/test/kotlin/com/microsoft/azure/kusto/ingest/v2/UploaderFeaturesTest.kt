// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.container.BlobUploadContainer
import com.microsoft.azure.kusto.ingest.v2.container.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.container.UploadResult
import com.microsoft.azure.kusto.ingest.v2.container.UploadSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploaderFeaturesTest : IngestV2TestBase(UploaderFeaturesTest::class.java) {

    private fun createUploadContainer(
        maxDataSize: Long = 4L * 1024 * 1024 * 1024,
        ignoreSizeLimit: Boolean = false,
        maxConcurrency: Int = 4
    ): BlobUploadContainer {
        val configCache = DefaultConfigurationCache(
            dmUrl = dmEndpoint,
            tokenCredential = tokenProvider,
            skipSecurityChecks = true
        )
        return BlobUploadContainer(
            configurationCache = configCache,
            maxDataSize = maxDataSize,
            ignoreSizeLimit = ignoreSizeLimit,
            maxConcurrency = maxConcurrency
        )
    }

    private fun createTestSource(sizeInBytes: Int, name: String): UploadSource {
        val data = ByteArray(sizeInBytes) { it.toByte() }
        return UploadSource(
            name = name,
            stream = ByteArrayInputStream(data),
            sizeBytes = sizeInBytes.toLong()
        )
    }

    @Test
    fun `test single upload - small file`() = runBlocking {
        logger.info("Testing single upload with small file")
        
        val uploader = createUploadContainer()
        val source = createTestSource(1024, "single_small_test.json")
        
        try {
            val blobUri = uploader.uploadAsync(source.name, source.stream)
            
            assertNotNull(blobUri, "Upload should return a blob URI")
            assertTrue(blobUri.isNotEmpty(), "Blob URI should not be empty")
            logger.info("Single upload successful: $blobUri")
        } catch (e: Exception) {
            logger.error("Single upload failed", e)
            throw e
        }
    }

    @Test
    fun `test single upload - large file`() = runBlocking {
        logger.info("Testing single upload with large file (10MB)")
        
        val uploader = createUploadContainer()
        val source = createTestSource(10 * 1024 * 1024, "single_large_test.json") // 10MB
        
        try {
            val blobUri = uploader.uploadAsync(source.name, source.stream)
            
            assertNotNull(blobUri, "Upload should return a blob URI")
            assertTrue(blobUri.isNotEmpty(), "Blob URI should not be empty")
            logger.info("Large single upload successful: $blobUri")
        } catch (e: Exception) {
            logger.error("Large single upload failed", e)
            throw e
        }
    }

    @Test
    fun `test batch upload - multiple files`() = runBlocking {
        logger.info("Testing batch upload with multiple files")
        
        val uploader = createUploadContainer()
        val sources = (1..5).map { index ->
            createTestSource(1024 * index, "batch_test_$index.json")
        }
        
        try {
            val uploadResults = uploader.uploadManyAsync(sources)
            
            assertEquals(5, uploadResults.successes.size + uploadResults.failures.size, "Should have 5 upload results")
            
            val successCount = uploadResults.successes.size
            val failureCount = uploadResults.failures.size
            
            logger.info("Batch upload results - Success: $successCount, Failure: $failureCount")
            
            uploadResults.successes.forEachIndexed { index, result ->
                assertNotNull(result.blobUrl, "Success result should have blob URL")
                logger.info("  [$index] Success: ${result.blobUrl}")
            }
            
            uploadResults.failures.forEachIndexed { index, result ->
                logger.warn("  [$index] Failure: ${result.errorCode} - ${result.errorMessage}")
            }
            
            assertTrue(successCount > 0, "At least some uploads should succeed")
            logger.info("Batch upload test completed")
        } catch (e: Exception) {
            logger.error("Batch upload failed", e)
            throw e
        }
    }

    @Test
    fun `test parallel processing with max concurrency`() = runBlocking {
        logger.info("Testing parallel processing with controlled concurrency")
        
        val uploader = createUploadContainer(maxConcurrency = 3)
        val sources = (1..10).map { index ->
            createTestSource(512 * 1024, "parallel_test_$index.json") // 512KB each
        }
        
        try {
            val startTime = System.currentTimeMillis()
            val uploadResults = uploader.uploadManyAsync(sources)
            val duration = System.currentTimeMillis() - startTime
            
            val totalResults = uploadResults.successes.size + uploadResults.failures.size
            assertEquals(10, totalResults, "Should have 10 upload results")
            
            val successCount = uploadResults.successes.size
            logger.info("Parallel upload: $successCount/$totalResults succeeded in ${duration}ms")
            logger.info("Average time per upload: ${duration / totalResults}ms")
            
            assertTrue(successCount > 0, "At least some parallel uploads should succeed")
            logger.info("Parallel processing test completed")
        } catch (e: Exception) {
            logger.error("Parallel upload failed", e)
            throw e
        }
    }

    @Test
    fun `test size validation - within limit`() = runBlocking {
        logger.info("Testing size validation with file within limit")
        
        val uploader = createUploadContainer(maxDataSize = 10 * 1024 * 1024) // 10MB limit
        val source = createTestSource(5 * 1024 * 1024, "size_valid_test.json") // 5MB file
        
        try {
            val blobUri = uploader.uploadAsync(source.name, source.stream)
            
            assertNotNull(blobUri, "Upload should succeed for file within size limit")
            logger.info("Size validation passed for file within limit")
        } catch (e: Exception) {
            logger.error("Size validation test failed", e)
            throw e
        }
    }

    @Test
    fun `test size validation - exceeds limit`() = runBlocking {
        logger.info("Testing size validation with file exceeding limit")
        
        val uploader = createUploadContainer(maxDataSize = 1 * 1024 * 1024) // 1MB limit
        val source = createTestSource(2 * 1024 * 1024, "size_exceed_test.json") // 2MB file
        
        try {
            uploader.uploadAsync(source.name, source.stream)
            logger.error("✗ Upload should have failed due to size limit")
            throw AssertionError("Expected size limit exception but upload succeeded")
        } catch (e: Exception) {
            logger.info("Size validation correctly rejected file exceeding limit: ${e.message}")
            assertTrue(
                e.message?.contains("size", ignoreCase = true) == true ||
                e.message?.contains("limit", ignoreCase = true) == true,
                "Error message should mention size/limit"
            )
        }
    }

    @Test
    fun `test size validation - ignore limit flag`() = runBlocking {
        logger.info("Testing size validation with ignore limit flag")
        
        val uploader = createUploadContainer(
            maxDataSize = 1 * 1024 * 1024, // 1MB limit
            ignoreSizeLimit = true
        )
        val source = createTestSource(2 * 1024 * 1024, "size_ignore_test.json") // 2MB file
        
        try {
            val blobUri = uploader.uploadAsync(source.name, source.stream)
            
            assertNotNull(blobUri, "Upload should succeed when size limit is ignored")
            logger.info("Size limit successfully bypassed with ignoreSizeLimit flag")
        } catch (e: Exception) {
            logger.error("Size validation ignore flag test failed", e)
            throw e
        }
    }

    @Test
    fun `test error codes - empty source list`() = runBlocking {
        logger.info("Testing error code for empty source list")
        
        val uploader = createUploadContainer()
        
        try {
            val results = uploader.uploadManyAsync(listOf())
            
            assertEquals(0, results.totalCount, "Empty source list should return empty results")
            assertTrue(results.allSucceeded, "Empty upload should be considered successful")
            logger.info("Empty source list handled correctly")
        } catch (e: Exception) {
            logger.error("Empty source test failed", e)
            throw e
        }
    }

    @Test
    fun `test upload results - success and failure tracking`() = runBlocking {
        logger.info("Testing upload result tracking")
        
        val uploader = createUploadContainer()
        
        // Mix of valid and potentially problematic uploads
        val sources = listOf(
            createTestSource(1024, "result_test_1.json"),
            createTestSource(2048, "result_test_2.json"),
            createTestSource(4096, "result_test_3.json")
        )
        
        try {
            val uploadResults = uploader.uploadManyAsync(sources)
            
            val totalResults = uploadResults.successes.size + uploadResults.failures.size
            assertEquals(3, totalResults, "Should have 3 results")

            uploadResults.successes.forEach { result ->
                assertNotNull(result.sourceName, "Success result should have source name")
                assertNotNull(result.blobUrl, "Success result should have blob URL")
                logger.info("  Success: name=${result.sourceName}, uri=${result.blobUrl}")
            }

            uploadResults.failures.forEach { result ->
                assertNotNull(result.sourceName, "Failure result should have source name")
                assertNotNull(result.errorCode, "Failure result should have error code")
                assertNotNull(result.errorMessage, "Failure result should have message")
                logger.info("  Failure: name=${result.sourceName}, error=${result.errorCode}")
            }
            
            logger.info("Upload result tracking verified")
        } catch (e: Exception) {
            logger.error("Result tracking test failed", e)
            throw e
        }
    }

    @Test
    fun `test error codes - comprehensive validation`() = runBlocking {
        logger.info("Testing comprehensive error code coverage")

        val errorCodes = UploadErrorCode.values()
        
        assertTrue(errorCodes.isNotEmpty(), "Error codes should be defined")
        logger.info("Available error codes:")
        errorCodes.forEach { code ->
            logger.info("  - ${code.name}: ${code.code} - ${code.description}")
        }
        
        // Verify critical error codes exist (using enum names)
        val criticalCodes = listOf(
            "SOURCE_IS_NULL",
            "SOURCE_SIZE_LIMIT_EXCEEDED",
            "UPLOAD_FAILED",
            "SOURCE_NOT_READABLE"
        )
        
        criticalCodes.forEach { expectedCode ->
            val exists = errorCodes.any { it.name == expectedCode }
            assertTrue(exists, "Critical error code $expectedCode should exist")
        }

        assertTrue(errorCodes.size >= 10, "Should have at least 10 error codes defined")
        
        logger.info("Error code validation completed - ${errorCodes.size} codes defined")
    }

    @Test
    fun `test max concurrency control`() = runBlocking {
        logger.info("Testing max concurrency parameter")
        
        val sources = (1..20).map { index ->
            createTestSource(256 * 1024, "concurrency_test_$index.json")
        }

        val concurrencyLevels = listOf(1, 5, 10)
        
        concurrencyLevels.forEach { concurrency ->
            logger.info("Testing with maxConcurrency=$concurrency")
            
            val uploader = createUploadContainer(maxConcurrency = concurrency)
            val startTime = System.currentTimeMillis()
            val uploadResults = uploader.uploadManyAsync(sources)
            val duration = System.currentTimeMillis() - startTime
            
            val totalResults = uploadResults.successes.size + uploadResults.failures.size
            val successCount = uploadResults.successes.size
            logger.info("  Concurrency $concurrency: $successCount/$totalResults succeeded in ${duration}ms")
        }
        
        logger.info("Max concurrency control test completed")
    }
}
