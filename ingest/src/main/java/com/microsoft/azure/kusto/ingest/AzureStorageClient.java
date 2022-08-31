// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.queue.QueueClient;
import com.microsoft.azure.kusto.data.Ensure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;

class AzureStorageClient {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int GZIP_BUFFER_SIZE = 16384;
    private static final int STREAM_BUFFER_SIZE = 16384;

    public AzureStorageClient() {
    }

    void postMessageToQueue(QueueClient queueClient, String content) {
        // Ensure
        Ensure.argIsNotNull(queueClient, "queueClient");
        Ensure.stringIsNotBlank(content, "content");

        byte[] bytesEncoded = java.util.Base64.getDecoder().decode(content.getBytes());
        queueClient.sendMessage(BinaryData.fromBytes(bytesEncoded));
    }

    public void azureTableInsertEntity(TableClient tableClient, TableEntity tableEntity) throws URISyntaxException {
        Ensure.argIsNotNull(tableClient, "tableClient");
        Ensure.argIsNotNull(tableEntity, "tableEntity");

        tableClient.createEntity(tableEntity);
    }

    void uploadLocalFileToBlob(File file, String blobName, BlobContainerClient container, boolean shouldCompress)
            throws IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", file.getPath(), blobName, container.getBlobContainerUrl());

        // Ensure
        Ensure.fileExists(file, "sourceFile");
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.argIsNotNull(container, "container");

        BlobClient blobClient = container.getBlobClient(blobName);
        if (shouldCompress) {
            compressAndUploadFileToBlob(file, blobClient);
        } else {
            uploadFileToBlob(file, blobClient);
        }
    }

    void compressAndUploadFileToBlob(File sourceFile, BlobClient blob) throws IOException {
        Ensure.fileExists(sourceFile, "sourceFile");
        Ensure.argIsNotNull(blob, "blob");

        try (InputStream fin = Files.newInputStream(sourceFile.toPath());
                GZIPOutputStream gzOut = new GZIPOutputStream(blob.getBlockBlobClient().getBlobOutputStream(true))) {
            copyStream(fin, gzOut, GZIP_BUFFER_SIZE);
        }
    }

    void uploadFileToBlob(File sourceFile, BlobClient blobClient) throws IOException {
        // Ensure
        Ensure.argIsNotNull(blobClient, "blob");
        Ensure.fileExists(sourceFile, "sourceFile");

        blobClient.uploadFromFile(sourceFile.getPath());
    }

    void uploadStreamToBlob(InputStream inputStream, String blobName, BlobContainerClient container, boolean shouldCompress)
            throws IOException, URISyntaxException {
        log.debug("uploadStreamToBlob: blobName: {}, storageUri: {}", blobName, container);

        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.argIsNotNull(container, "container");

        BlobClient blobClient = container.getBlobClient(blobName);
        if (shouldCompress) {
            compressAndUploadStream(inputStream, blobClient);
        } else {
            uploadStream(inputStream, blobClient);
        }
    }

    void uploadStream(InputStream inputStream, BlobClient blob) throws IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        OutputStream blobOutputStream = blob.getBlockBlobClient().getBlobOutputStream(true);
        copyStream(inputStream, blobOutputStream, STREAM_BUFFER_SIZE);
        blobOutputStream.close();
    }

    void compressAndUploadStream(InputStream inputStream, BlobClient blob) throws IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        try (GZIPOutputStream gzout = new GZIPOutputStream(blob.getBlockBlobClient().getBlobOutputStream(true))) {
            copyStream(inputStream, gzout, GZIP_BUFFER_SIZE);
        }
    }

    private void copyStream(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }
    }
}
