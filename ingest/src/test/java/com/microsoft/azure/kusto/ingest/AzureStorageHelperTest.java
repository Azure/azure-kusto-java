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

class AzureStorageHelperTest {
    static private AzureStorageHelper azureStorageHelper = new AzureStorageHelper();
    static private AzureStorageHelper azureStorageHelperSpy;

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

        azureStorageHelperSpy = spy(azureStorageHelper);
        doNothing().when(azureStorageHelperSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));
        doNothing().when(azureStorageHelperSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));
        doNothing().when(azureStorageHelperSpy).compressAndUploadStream(any(InputStream.class), any(CloudBlockBlob.class));
        doNothing().when(azureStorageHelperSpy).uploadStream(any(InputStream.class), any(CloudBlockBlob.class));
    }

    @Test
    void postMessageToQueueThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.postMessageToQueue(null, "content"));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.postMessageToQueue("queuePath", null));
    }

    @Test
    void azureTableInsertEntityThrowExceptionWhenArgumentIsNull() {
        TableServiceEntity serviceEntity = new TableServiceEntity();
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.azureTableInsertEntity("tableUri", null));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.azureTableInsertEntity(null, serviceEntity));
    }

    @Test
    void uploadLocalFileToBlobCompressAndUploadWhenFileIsUncompressed() throws IOException, StorageException, URISyntaxException {
        azureStorageHelperSpy.uploadLocalFileToBlob(testFilePath, "blobName", "https://ms.com/blob");
        verify(azureStorageHelperSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));
    }

    @Test
    void uploadLocalFileToBlobUploadFileWhenFileIsCompressed() throws IOException, StorageException, URISyntaxException {
        azureStorageHelperSpy.uploadLocalFileToBlob(testFilePathCompressed, "blobName", "https://ms.com/blob");
        verify(azureStorageHelperSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.uploadLocalFileToBlob(null, "blobName", "storageUri"));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.uploadLocalFileToBlob("filePath", null, "storageUri"));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.uploadLocalFileToBlob("filePath", "blobName", null));
    }

    @Test
    void uploadLocalFileToBlobThrowExceptionWhenFileDoesNotExist() {
        String notExistingFilePath = "not.existing.file.path";
        assertThrows(IOException.class,
                () -> azureStorageHelper.uploadLocalFileToBlob(notExistingFilePath, "blobName", "storageUri"));
    }

    @Test
    void uploadStreamToBlobNotCompressModeCallsStreamToBlob() throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            azureStorageHelperSpy.uploadStreamToBlob(stream, "blobName", "https://ms.com/storageUri", false);
            verify(azureStorageHelperSpy).uploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void uploadStreamToBlobCompressModeCallsCompressAndUploadStreamToBlob() throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            azureStorageHelperSpy.uploadStreamToBlob(stream, "blobName", "https://ms.com/storageUri", true);
            verify(azureStorageHelperSpy).compressAndUploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void uploadStreamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.uploadStreamToBlob(null, "blobName", "storageUri", false));

            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.uploadStreamToBlob(stream, null, "storageUri", false));

            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.uploadStreamToBlob(stream, "blobName", null, false));
        }
    }

    @Test
    void compressAndUploadFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.compressAndUploadFileToBlob(null, blob));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.compressAndUploadFileToBlob(testFilePath, null));
    }

    @Test
    void uploadFileToBlobThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.uploadFileToBlob(null, blob));

        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.uploadFileToBlob(testFile, null));
    }

    @Test
    void streamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.uploadStream(null, blob));

            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.uploadStream(stream, null));
        }
    }

    @Test
    void compressAndStreamToBlobThrowExceptionWhenArgumentIsNull() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.compressAndUploadStream(null, blob));

            assertThrows(IllegalArgumentException.class,
                    () -> azureStorageHelper.compressAndUploadStream(stream, null));
        }
    }

    @Test
    void getBlobPathWithSasThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.getBlobPathWithSas(null));
    }

    @Test
    void getBlobSizeThrowExceptionWhenArgumentIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> azureStorageHelper.getBlobSize(null));
    }
}