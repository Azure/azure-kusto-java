package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class AzureStorageClientTest {
    static private AzureStorageClient azureStorageClient = new AzureStorageClient();
    static private AzureStorageClient azureStorageClientSpy;

    static private String testFilePath;
    static private File testFile;
    static private String testFilePathCompressed;
    static private CloudBlockBlob blob;

    @BeforeAll
    static void setUp() throws IOException, StorageException, URISyntaxException {
        testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
        testFile = new File(testFilePath);
        testFilePathCompressed = Paths.get("src", "test", "resources", "testdata.json.gz").toString();
        blob = new CloudBlockBlob(new URI("https://ms.com/storageUri"));

        azureStorageClientSpy = spy(azureStorageClient);
        doNothing().when(azureStorageClientSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));
        doNothing().when(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));
        doNothing().when(azureStorageClientSpy)
                .compressAndUploadStream(any(InputStream.class), any(CloudBlockBlob.class));
        doNothing().when(azureStorageClientSpy).uploadStream(any(InputStream.class), any(CloudBlockBlob.class));
    }

    @Test
    void postMessageToQueueThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue(null, "content"));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue("queuePath", null));
    }

    @Test
    void azureTableInsertEntityThrowExceptionWhenArgumentIsNull() {
        TableServiceEntity serviceEntity = new TableServiceEntity();
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity("tableUri", null));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity(null, serviceEntity));
    }

    @Test
    void uploadLocalFileToBlobCompressAndUploadWhenFileIsUncompressed()
            throws IOException, StorageException, URISyntaxException {
        azureStorageClientSpy.uploadLocalFileToBlob(testFilePath, "blobName", "https://ms.com/blob");
        verify(azureStorageClientSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));
    }

    @Test
    void uploadLocalFileToBlobUploadFileWhenFileIsCompressed()
            throws IOException, StorageException, URISyntaxException {
        azureStorageClientSpy.uploadLocalFileToBlob(testFilePathCompressed, "blobName", "https://ms.com/blob");
        verify(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(null, "blobName", "storageUri"));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob("filePath", null, "storageUri"));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob("filePath", "blobName", null));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenFileDoesNotExist() {
        String notExistingFilePath = "not.existing.file.path";
        assertThrows(
                IOException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(notExistingFilePath, "blobName", "storageUri"));
    }

    @Test
    void uploadStreamToBlobNotCompressModeCallsStreamToBlob() throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName", "https://ms.com/storageUri", false);
            verify(azureStorageClientSpy).uploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void uploadStreamToBlobCompressModeCallsCompressAndUploadStreamToBlob()
            throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName", "https://ms.com/storageUri", true);
            verify(azureStorageClientSpy).compressAndUploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void uploadStreamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(null, "blobName", "storageUri", false));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, null, "storageUri", false));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, "blobName", null, false));
        }
    }

    @Test
    void compressAndUploadFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(null, blob));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(testFilePath, null));
    }

    @Test
    void uploadFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(null, blob));

        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(testFile, null));
    }

    @Test
    void streamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStream(null, blob));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStream(stream, null));
        }
    }

    @Test
    void compressAndStreamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.compressAndUploadStream(null, blob));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.compressAndUploadStream(stream, null));
        }
    }

    @Test
    void getBlobPathWithSasThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.getBlobPathWithSas(null));
    }

    @Test
    void getBlobSizeThrowExceptionWhenArgumentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.getBlobSize(null));
    }
}