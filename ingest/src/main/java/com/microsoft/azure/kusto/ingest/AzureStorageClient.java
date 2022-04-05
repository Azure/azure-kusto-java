// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.queue.QueueClient;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.source.CompressionType;

import org.apache.commons.codec.binary.Base64;
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

public class AzureStorageClient {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static final int GZIP_BUFFER_SIZE = 16384;
    static final int STREAM_BUFFER_SIZE = 16384;

    void postMessageToQueue(QueueClient queueClient, String content) throws URISyntaxException {
        // Ensure
        Ensure.argIsNotNull(queueClient, "queueClient");
        Ensure.stringIsNotBlank(content, "content");

        byte[] bytesEncoded = Base64.encodeBase64(content.getBytes());
        queueClient.sendMessage(BinaryData.fromBytes(bytesEncoded));
    }

    public void azureTableInsertEntity(TableClient tableClient, TableEntity tableEntity) throws URISyntaxException {
        Ensure.argIsNotNull(tableClient, "tableClient");
        Ensure.argIsNotNull(tableEntity, "tableEntity");

        tableClient.createEntity(tableEntity);
    }

    BlobClient uploadLocalFileToBlob(File file, String blobName, BlobContainerClient container, boolean shouldCompress)
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

        return blobClient;
    }

    void uploadFileToBlob(File sourceFile, BlobClient blobClient) throws IOException {
        // Ensure
        Ensure.argIsNotNull(blobClient, "blob");
        Ensure.fileExists(sourceFile, "sourceFile");

        blobClient.uploadFromFile(sourceFile.getPath());
    }

    public void compressAndUploadFileToBlob(File sourceFile, BlobClient blob) throws IOException {
        Ensure.fileExists(sourceFile, "sourceFile");
        Ensure.argIsNotNull(blob, "blob");

        try (InputStream fin = Files.newInputStream(sourceFile.toPath());
             GZIPOutputStream gzOut = new GZIPOutputStream(blob.getBlockBlobClient().getBlobOutputStream())) {
            copyStream(fin, gzOut, GZIP_BUFFER_SIZE);
        }
    }

    BlobClient uploadStreamToBlob(InputStream inputStream, String blobName, BlobContainerClient container, boolean shouldCompress)
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

        return blobClient;
    }

    void uploadStream(InputStream inputStream, BlobClient blob) throws IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        OutputStream blobOutputStream = blob.getBlockBlobClient().getBlobOutputStream();
        copyStream(inputStream, blobOutputStream, STREAM_BUFFER_SIZE);
        blobOutputStream.close();
    }

    void compressAndUploadStream(InputStream inputStream, BlobClient blob) throws IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        try (GZIPOutputStream gzout = new GZIPOutputStream(blob.getBlockBlobClient().getBlobOutputStream())) {
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

    String getBlobPathWithSas(String blobSas, String blobName) {
        return blobSas.concat(blobName);
    }

    // We don't support compression of Parquet and Orc files
    static boolean shouldCompress(CompressionType sourceCompressionType, String dataFormat) {
        return sourceCompressionType == null
                && (dataFormat == null ||
                (!dataFormat.equals(IngestionProperties.DataFormat.parquet.name())
                        && !dataFormat.equals(IngestionProperties.DataFormat.orc.name())));
    }

    public static TableClient TableClientFromUrl(String url) {
        String[] parts = url.split("\\?");
        int tableNameIndex = parts[0].lastIndexOf('/');
        String tableName = parts[0].substring(tableNameIndex + 1);

        return new TableClientBuilder()
                .endpoint(parts[0].substring(0, tableNameIndex))
                .sasToken(parts[1])
                .tableName(tableName)
                .buildClient();
    }
}