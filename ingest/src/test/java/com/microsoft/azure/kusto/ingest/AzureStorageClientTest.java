package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    void postMessageToQueueThrowExceptionWhenQueuePathIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue(null, "content"));
    }

    @Test
    void postMessageToQueueThrowExceptionWhenContentIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue("queuePath", null));
    }

    @Test
    void azureTableInsertEntityThrowExceptionWhenEntityIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity("tableUri", null));
    }

    @Test
    void azureTableInsertEntityThrowExceptionWhenTableUriIsNull() {
        TableServiceEntity serviceEntity = new TableServiceEntity();
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
    void uploadLocalFileToBlobThrowExceptionWhenFilePathIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(null, "blobName", "storageUri"));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenBlobNameIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob("filePath", null, "storageUri"));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenStorageUriIsNull() {
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
    void uploadStreamToBlobThrowExceptionWhenInputStreamIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(null, "blobName", "storageUri", false));
        }
    }

    @Test
    void uploadStreamToBlobThrowExceptionWhenBlobNameIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, null, "storageUri", false));
        }
    }

    @Test
    void uploadStreamToBlobThrowExceptionWhenStorageUriIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, "blobName", null, false));
        }
    }

    @Test
    void compressAndUploadFileToBlobThrowExceptionWhenFilePathIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(null, blob));
    }

    @Test
    void compressAndUploadFileToBlobThrowExceptionWhenBlobIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(testFilePath, null));
    }

    @Test
    void uploadFileToBlobThrowExceptionWhenSourceFileIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(null, blob));
    }

    @Test
    void uploadFileToBlobThrowExceptionWhenBlobIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(testFile, null));
    }

    @Test
    void streamToBlobThrowExceptionWhenInputStreamIsNull() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStream(null, blob));

    }

    @Test
    void streamToBlobThrowExceptionWhenBlobIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStream(stream, null));
        }
    }

    @Test
    void compressAndStreamToBlobThrowExceptionWhenInputStreamIsNull() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadStream(null, blob));

    }

    @Test
    void compressAndStreamToBlobThrowExceptionWhenBlobIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
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