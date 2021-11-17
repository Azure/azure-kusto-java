// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Blob;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class AzureStorageClientTest {
    static private final AzureStorageClient azureStorageClient = new AzureStorageClient();
    static private AzureStorageClient azureStorageClientSpy;

    static private String testFilePath;
    static private File testFile;
    static private String testFilePathCompressed;
    static private File testFileCompressed;
    static private BlobClient blob;

    @BeforeAll
    static void setUp() {
        testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
        testFile = new File(testFilePath);
        testFilePathCompressed = Paths.get("src", "test", "resources", "testdata.json.gz").toString();
        testFileCompressed = new File(testFilePathCompressed);
        blob = new BlobClientBuilder().endpoint("https://ms.com/storageUri/blobName")
                .buildClient();
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
                () -> azureStorageClient.azureTableInsertEntity("tableUri", null));
    }

    @Test
    void PostMessageToQueue_NullTableUri_IllegalArgumentException() {
        TableEntity serviceEntity = mock(TableEntity.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.azureTableInsertEntity(null, serviceEntity));
    }

    @Test
    void UploadLocalFileToBlob_UncompressedFile_CompressAndUploadFileToBlobIsCalled()
            throws IOException {
        doNothing().when(azureStorageClientSpy).compressAndUploadFileToBlob(any(File.class), any(BlobClient.class));

        azureStorageClientSpy.uploadLocalFileToBlob(testFile, "blobName", new BlobContainerClientBuilder().endpoint("https://ms.com/blob").buildClient(), true);
        verify(azureStorageClientSpy).compressAndUploadFileToBlob(any(), any(BlobClient.class));
    }

    @Test
    void UploadLocalFileToBlob_CompressedFile_UploadFileToBlobIsCalled()
            throws IOException {
        doNothing().when(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(BlobClient.class));

        azureStorageClientSpy.uploadLocalFileToBlob(testFileCompressed, "blobName",
                "https://ms.com/blob", IngestionProperties.DataFormat.csv);
        verify(azureStorageClientSpy).uploadFileToBlob(any(File.class), any(BlobClient.class));
    }

    @Test
    void UploadLocalFileToBlob_NullFilePath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(null, "blobName", "storageUri", IngestionProperties.DataFormat.csv));
    }

    @Test
    void UploadLocalFileToBlob_NullBlobName_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(testFile, null, "storageUri", IngestionProperties.DataFormat.csv));
    }

    @Test
    void UploadLocalFileToBlob_NullStorageUri_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(testFile, "blobName", null, IngestionProperties.DataFormat.csv));
    }

    @Test
    void UploadLocalFileToBlob_FileDoesNotExist_IOException() {
        File notExistingFile = new File("not.existing.file.path");
        assertThrows(
                IOException.class,
                () -> azureStorageClient.uploadLocalFileToBlob(notExistingFile, "blobName", "storageUri", IngestionProperties.DataFormat.csv));
    }

    @Test
    void UploadStreamToBlob_NotCompressMode_UploadStreamIsCalled()
            throws IOException, URISyntaxException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            doNothing().when(azureStorageClientSpy).uploadStream(any(InputStream.class), any(BlobClient.class));

            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName",
                    new BlobContainerClientBuilder().endpoint("https://ms.com/storageUri").buildClient(), false);
            verify(azureStorageClientSpy).uploadStream(isA(InputStream.class), isA(BlobClient.class));
        }
    }

    @Test
    void UploadStreamToBlob_CompressMode_CompressAndUploadStreamIsCalled()
            throws IOException, URISyntaxException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            doNothing().when(azureStorageClientSpy)
                    .compressAndUploadStream(any(InputStream.class), any(BlobClient.class));
            azureStorageClientSpy.uploadStreamToBlob(stream, "blobName",
                    new BlobContainerClientBuilder().endpoint("https://ms.com/storageUri").buildClient(), true);
            verify(azureStorageClientSpy).compressAndUploadStream(isA(InputStream.class), isA(BlobClient.class));
        }
    }

    @Test
    void UploadStreamToBlob_NullInputStream_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.uploadStreamToBlob(null, "blobName", new BlobContainerClientBuilder().endpoint("storageUri").buildClient(), false));

    }

    @Test
    void UploadStreamToBlob_NullBlobName_IllegalArgumentException() throws IOException {
        try (InputStream stream = new FileInputStream(testFilePath)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> azureStorageClient.uploadStreamToBlob(stream, null, new BlobContainerClientBuilder().endpoint("storageUri").buildClient(), false));
        }
    }

    @Test
    void UploadStreamToBlob_NullStorageUri_IllegalArgumentException() throws IOException {
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
                () -> azureStorageClient.compressAndUploadFileToBlob(testFile, null));
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
    void GetBlobSize_NullBlobPath_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> azureStorageClient.getBlobSize(null));
    }
}