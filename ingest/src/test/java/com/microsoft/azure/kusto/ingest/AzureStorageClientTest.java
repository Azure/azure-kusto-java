// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class AzureStorageClientTest {
    static private final AzureStorageClient azureStorageClient = new AzureStorageClient();
    static private AzureStorageClient azureStorageClientSpy;

    static private String testFilePath;
    static private File testFile;
    static private String testFilePathCompressed;
    static private CloudBlockBlob blob;

    @BeforeAll
    static void setUp() throws StorageException, URISyntaxException {
        testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
        testFile = new File(testFilePath);
        testFilePathCompressed = Paths.get("src", "test", "resources", "testdata.json.gz").toString();
        blob = new CloudBlockBlob(new URI("https://testcontosourl.com/storageUrl"));
    }

    @BeforeEach
    void setUpEach() {
        azureStorageClientSpy = spy(azureStorageClient);
    }

    @Test
    void PostMessageToQueue_NullQueuePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue(null, "content"));
    }

    @Test
    void PostMessageToQueue_NullContent_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.postMessageToQueue("queuePath", null));
    }

    @Test
    void PostMessageToQueue_NullEntity_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity("tableUrl", null));
    }

    @Test
    void PostMessageToQueue_NullTableUrl_IllegalArgumentException() {
        TableServiceEntity serviceEntity = new TableServiceEntity();
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity(null, serviceEntity));
    }

    @Test
    void UploadLocalFileToBlob_UncompressedFile_CompressAndUploadFileToBlobIsCalled()
        throws IOException, StorageException, URISyntaxException {
        doNothing().when(azureStorageClientSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));

        azureStorageClientSpy.uploadLocalFileToBlob(testFilePath, "blobName", "https://testcontosourl.com/blob", IngestionProperties.DataFormat.CSV);
        verify(azureStorageClientSpy).compressAndUploadFileToBlob(anyString(), any(CloudBlockBlob.class));
    }

    @Test
    void UploadLocalFileToBlob_CompressedFile_UploadFileToBlobIsCalled()
        throws IOException, StorageException, URISyntaxException {
        doNothing().when(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));

        azureStorageClientSpy.uploadLocalFileToBlob(testFilePathCompressed, "blobName", "https://testcontosourl.com/blob", IngestionProperties.DataFormat.CSV);
        verify(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(CloudBlockBlob.class));
    }

    @Test
    void UploadLocalFileToBlob_NullFilePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(null, "blobName", "storageUrl", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void UploadLocalFileToBlob_NullBlobName_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(testFilePath, null, "storageUrl", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void UploadLocalFileToBlob_NullStorageUrl_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(testFilePath, "blobName", null, IngestionProperties.DataFormat.CSV));
    }

    @Test
    void UploadLocalFileToBlob_FileDoesNotExist_IOException() {
        String notExistingFilePath = "not.existing.file.path";
        assertThrows(
                IOException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(notExistingFilePath, "blobName", "storageUrl", IngestionProperties.DataFormat.CSV));
    }

    @Test
    void UploadStreamToBlob_NotCompressMode_UploadStreamIsCalled()
        throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            doNothing().when(azureStorageClientSpy).uploadStream(any(InputStream.class), any(CloudBlockBlob.class));

            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName", "https://ms.com/storageUrl", false);
            verify(azureStorageClientSpy).uploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void UploadStreamToBlob_CompressMode_CompressAndUploadStreamIsCalled()
        throws IOException, URISyntaxException, StorageException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            doNothing().when(azureStorageClientSpy)
                    .compressAndUploadStream(any(InputStream.class), any(CloudBlockBlob.class));
            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName", "https://testcontosourl.com/storageUrl", true);
            verify(azureStorageClientSpy).compressAndUploadStream(isA(InputStream.class), isA(CloudBlockBlob.class));
        }
    }

    @Test
    void UploadStreamToBlob_NullInputStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStreamToBlob(null, "blobName", "storageUrl", false));

    }

    @Test
    void UploadStreamToBlob_NullBlobName_IllegalArgumentException() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, null, "storageUrl", false));
        }
    }

    @Test
    void UploadStreamToBlob_NullStorageUrl_IllegalArgumentException() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, "blobName", null, false));
        }
    }

    @Test
    void CompressAndUploadFileToBlob_NullFilePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(null, blob));
    }

    @Test
    void CompressAndUploadFileToBlob_NullBlob_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadFileToBlob(testFilePath, null));
    }

    @Test
    void UploadFileToBlob_NullSourceFile_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(null, blob));
    }

    @Test
    void UploadFileToBlob_NullBlob_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadFileToBlob(testFile, null));
    }

    @Test
    void UploadStream_NullInputStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStream(null, blob));

    }

    @Test
    void UploadStream_NullBlob_IllegalArgumentException() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStream(stream, null));
        }
    }

    @Test
    void CompressAndStream_NullStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.compressAndUploadStream(null, blob));

    }

    @Test
    void CompressAndStream_NullBlob_IllegalArgumentException() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.compressAndUploadStream(stream, null));
        }
    }

    @Test
    void GetBlobPathWithSas_NullCloudBlockBlob_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.getBlobPathWithSas(null));
    }

    @Test
    void GetBlobSize_NullBlobPath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.getBlobSize(null));
    }
}
