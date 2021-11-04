// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.table.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class AzureStorageClientV2 {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int GZIP_BUFFER_SIZE = 16384;

    public void azureTableInsertEntity(String tableStatusUri, TableEntity entity) throws URISyntaxException, InvalidKeyException, StorageException {
        String[] split = tableStatusUri.split("\\?");
        URI tableUri = new URI(split[0]);
        CloudTableClient tableClient2 = new CloudTableClient(tableUri, new StorageCredentialsSharedAccessSignature(split[1]));
        CloudTable s = tableClient2.getTableReference(removeTrailSlash(tableUri.getPath()));
        TableOperation insert = TableOperation.insert(entity);
        s.execute(insert);
    }

    String removeTrailSlash(String tableUri){
        if (tableUri.startsWith("/")){
           return  tableUri.substring(1);
        }
        return tableUri;
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri, boolean shouldCompress, boolean v1)
            throws URISyntaxException, StorageException, IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", filePath, blobName, storageUri);

        // Ensure
        Ensure.fileExists(filePath);
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.stringIsNotBlank(storageUri, "storageUri");

        NettyAsyncHttpClientBuilder nettyAsyncHttpClientBuilder = new NettyAsyncHttpClientBuilder().
//                readTimeout(Duration.ofHours(1)).
//                writeTimeout(Duration.ofHours(1)).
//                connectTimeout(Duration.ofHours(1)).
                responseTimeout(Duration.ofHours(1));
        BlobContainerClient blobContainerClient2 = new BlobContainerClientBuilder()
                .endpoint(storageUri)
                .httpClient(nettyAsyncHttpClientBuilder.build())
                .buildClient();
        BlobClient blobClient = blobContainerClient2.getBlobClient(blobName);
        if (shouldCompress) {
            compressAndUploadFileToBlob(filePath, blobClient);
//            compressAndUploadFileToBlob(filePath, blob);
        } else {
            File file = new File(filePath);
            blobClient.uploadFromFile(file.getPath());
        }

        return null;
    }


    private void compressAndUploadFileToBlob(String filePath, BlobClient blob) throws IOException {
        Ensure.fileExists(filePath);
        Ensure.argIsNotNull(blob, "blob");

        try (InputStream fin = Files.newInputStream(Paths.get(filePath));
            GZIPOutputStream gzout = new GZIPOutputStream(blob.getBlockBlobClient().getBlobOutputStream())) {
            copyStream(fin, gzout, GZIP_BUFFER_SIZE);
        }
    }

    private void copyStream(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }
    }

    long getBlobSize(String blobPath) throws StorageException, URISyntaxException {
        Ensure.stringIsNotBlank(blobPath, "blobPath");

        CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
        blockBlob.downloadAttributes();
        return blockBlob.getProperties().getLength();
    }
}