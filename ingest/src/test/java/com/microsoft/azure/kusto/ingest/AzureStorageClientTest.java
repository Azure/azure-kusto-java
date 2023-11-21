// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.queue.QueueClient;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.microsoft.azure.kusto.ingest.IngestClientBase.shouldCompress;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class AzureStorageClientTest {
    static private final AzureStorageClient azureStorageClient = new AzureStorageClient();
    static private AzureStorageClient azureStorageClientSpy;

    static private String testFilePath;
    static private File testFile;
    static private File testFileCompressed;
    static private BlobClient blob;

    @BeforeAll
    static void setUp() {
        testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
        testFile = new File(testFilePath);
        String testFilePathCompressed = Paths.get("src", "test", "resources", "testdata.json.gz").toString();
        testFileCompressed = new File(testFilePathCompressed);
        blob = TestUtils.containerWithSasFromContainerName("storageUrl").getContainer().getBlobClient("bloby");
    }

    @BeforeEach
    void setUpEach() {
        azureStorageClientSpy = spy(azureStorageClient);
    }

    void uploadLocalFileToBlob(File file, String blobName, String storageUri, IngestionProperties.DataFormat dataFormat)
            throws IOException {
        Ensure.fileExists(file, "file");
        CompressionType sourceCompressionType = IngestionUtils.getCompression(file.getPath());
        azureStorageClientSpy.uploadLocalFileToBlob(file, blobName, new BlobContainerClientBuilder().endpoint(storageUri).buildClient(),
                shouldCompress(sourceCompressionType, dataFormat));
    }

    @Test
    void postMessageToQueue_NullQueuePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue(null, "content"));
    }

    @Test
    void postMessageToQueue_NullContent_IllegalArgumentException() {
        QueueClient queue = TestUtils.queueWithSasFromQueueName("queue1").getQueue();
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue(queue, null));
    }

    @Test
    void postMessageToQueue_NullEntity_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity(mock(TableClient.class), null));
    }

    @Test
    void postMessageToQueue_NullTableUri_IllegalArgumentException() {
        TableEntity serviceEntity = mock(TableEntity.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity(null, serviceEntity));
    }

    @Test
    void uploadLocalFileToBlob_UncompressedFile_CompressAndUploadFileToBlobIsCalled()
            throws IOException {
        doNothing().when(azureStorageClientSpy).compressAndUploadFileToBlob(any(File.class), any(BlobClient.class));

        azureStorageClientSpy.uploadLocalFileToBlob(testFile, "blobName",
                new BlobContainerClientBuilder().endpoint("https://testcontosourl.com/blob").buildClient(), true);
        verify(azureStorageClientSpy).compressAndUploadFileToBlob(any(), any(BlobClient.class));
    }

    @Test
    void uploadLocalFileToBlob_CompressedFile_UploadFileToBlobIsCalled()
            throws IOException {
        doNothing().when(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(BlobClient.class));

        uploadLocalFileToBlob(testFileCompressed, "blobName",
                "https://testcontosourl.com/blob", IngestionProperties.DataFormat.CSV);
        verify(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(BlobClient.class));
    }

    @Test
    void uploadLocalFileToBlob_NullFilePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> uploadLocalFileToBlob(null, "blobName", "storageUri", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void UploadLocalFileToBlob_NullBlobName_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> uploadLocalFileToBlob(testFile, null, "storageUri", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void uploadLocalFileToBlob_NullStorageUri_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> uploadLocalFileToBlob(testFile, "blobName", null, IngestionProperties.DataFormat.CSV));
    }

    @Test
    void uploadLocalFileToBlob_FileDoesNotExist_IOException() {
        File notExistingFile = new File("not.existing.file.path");
        assertThrows(
                IOException.class,
                () -> uploadLocalFileToBlob(notExistingFile, "blobName", "storageUri", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void uploadStreamToBlob_NotCompressMode_UploadStreamIsCalled()
            throws IOException, URISyntaxException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            doNothing().when(azureStorageClientSpy).uploadStream(any(InputStream.class), any(BlobClient.class));

            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName",
                    new BlobContainerClientBuilder().endpoint("https://ms.com/storageUrl").buildClient(), false);
            verify(azureStorageClientSpy).uploadStream(isA(InputStream.class), isA(BlobClient.class));
        }
    }

    @Test
    void uploadStreamToBlob_CompressMode_CompressAndUploadStreamIsCalled()
            throws IOException, URISyntaxException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            doNothing().when(azureStorageClientSpy)
                    .compressAndUploadStream(any(InputStream.class), any(BlobClient.class));
            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName",
                    new BlobContainerClientBuilder().endpoint("https://ms.com/storageUrl").buildClient(), true);
            verify(azureStorageClientSpy).compressAndUploadStream(isA(InputStream.class), isA(BlobClient.class));
        }
    }

    @Test
    void UploadStreamToBlob_NullInputStream_IllegalArgumentException() {
        BlobContainerClient container = new BlobContainerClientBuilder().endpoint("https://blobPath.blob.core.windows.net/container/blob").buildClient();
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStreamToBlob(null, "blobName", container, false));
    }

    @Test
    void uploadStreamToBlob_NullBlobName_IllegalArgumentException() throws IOException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            BlobContainerClient storageUrl = new BlobContainerClientBuilder().endpoint("https://blobPath.blob.core.windows.net/container/blob").buildClient();
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, null, storageUrl, false));
        }
    }

    @Test
    void uploadStreamToBlob_NullStorageUri_IllegalArgumentException() throws IOException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, "blobName", null, false));
        }
    }

    @Test
    void compressAndUploadFileToBlob_NullFilePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(null, blob));
    }

    @Test
    void compressAndUploadFileToBlob_NullBlob_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(testFile, null));
    }

    @Test
    void uploadFileToBlob_NullSourceFile_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(null, blob));
    }

    @Test
    void uploadFileToBlob_NullBlob_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(testFile, null));
    }

    @Test
    void uploadStream_NullInputStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStream(null, blob));

    }

    @Test
    void uploadStream_NullBlob_IllegalArgumentException() throws IOException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStream(stream, null));
        }
    }

    @Test
    void compressAndStream_NullStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadStream(null, blob));

    }

    @Test
    void compressAndStream_NullBlob_IllegalArgumentException() throws IOException {
        try (InputStream stream = Files.newInputStream(Paths.get(testFilePath))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.compressAndUploadStream(stream, null));
        }
    }
}
