// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.BinaryData;
import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.implementation.models.TableServiceErrorException;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.queue.QueueAsyncClient;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;

public class AzureStorageClient {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public AzureStorageClient() {
    }

    Mono<Void> postMessageToQueue(QueueAsyncClient queueAsyncClient, String content) {
        Ensure.argIsNotNull(queueAsyncClient, "queueAsyncClient");
        Ensure.stringIsNotBlank(content, "content");

        byte[] bytesEncoded = Base64.encodeBase64(content.getBytes());
        return queueAsyncClient.sendMessage(BinaryData.fromBytes(bytesEncoded)).then();
    }

    public Mono<Void> azureTableInsertEntity(TableAsyncClient tableAsyncClient, TableEntity tableEntity) throws TableServiceErrorException {
        Ensure.argIsNotNull(tableAsyncClient, "tableAsyncClient");
        Ensure.argIsNotNull(tableEntity, "tableEntity");

        return tableAsyncClient.createEntity(tableEntity);
    }

    Mono<Void> uploadLocalFileToBlob(File file, String blobName, BlobContainerAsyncClient asyncContainer, boolean shouldCompress) throws IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", file.getPath(), blobName, asyncContainer.getBlobContainerUrl());

        Ensure.fileExists(file, "sourceFile");
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.argIsNotNull(asyncContainer, "asyncContainer");

        BlobAsyncClient blobAsyncClient = asyncContainer.getBlobAsyncClient(blobName);
        if (shouldCompress) {
            return compressAndUploadFileToBlob(file, blobAsyncClient);
        } else {
            return uploadFileToBlob(file, blobAsyncClient);
        }
    }

    Mono<Void> compressAndUploadFileToBlob(File sourceFile, BlobAsyncClient blobAsyncClient) throws IOException {
        Ensure.fileExists(sourceFile, "sourceFile");
        Ensure.argIsNotNull(blobAsyncClient, "blobAsyncClient");

        return Mono.defer(() -> {
            try {
                InputStream inputStream = Files.newInputStream(sourceFile.toPath());
                return IngestionUtils.compressStream(inputStream, false)
                        .flatMap(bytes -> blobAsyncClient.getBlockBlobAsyncClient()
                                .upload(BinaryData.fromStream(bytes, (long) bytes.available()), true));
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }).then();
    }

    Mono<Void> uploadFileToBlob(File sourceFile, BlobAsyncClient blobAsyncClient) throws IOException {
        Ensure.argIsNotNull(blobAsyncClient, "blob");
        Ensure.fileExists(sourceFile, "sourceFile");

        return blobAsyncClient.uploadFromFile(sourceFile.getPath());
    }

    Mono<Integer> uploadStreamToBlob(InputStream inputStream,
            String blobName,
            BlobContainerAsyncClient asyncContainer,
            boolean shouldCompress) {
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.argIsNotNull(asyncContainer, "asyncContainer");

        log.debug("uploadStreamToBlob: blobName: {}, storageUri: {}", blobName, asyncContainer.getBlobContainerUrl());

        BlobAsyncClient blobAsyncClient = asyncContainer.getBlobAsyncClient(blobName);
        if (shouldCompress) {
            return compressAndUploadStream(inputStream, blobAsyncClient);
        } else {
            return uploadStream(inputStream, blobAsyncClient);
        }
    }

    // Returns original stream size
    Mono<Integer> uploadStream(InputStream inputStream, BlobAsyncClient blobAsyncClient) {
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blobAsyncClient, "blobAsyncClient");

        IngestionUtils.IntegerHolder size = new IngestionUtils.IntegerHolder();

        return IngestionUtils.toByteArray(inputStream)
                .flatMap(bytes -> {
                    size.add(bytes.length);
                    return blobAsyncClient.getBlockBlobAsyncClient().upload(BinaryData.fromBytes(bytes), true);
                })
                .thenReturn(size.getValue());
    }

    // Returns original stream size
    Mono<Integer> compressAndUploadStream(InputStream inputStream, BlobAsyncClient blobAsyncClient) {
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blobAsyncClient, "blobAsyncClient");

        IngestionUtils.IntegerHolder size = new IngestionUtils.IntegerHolder();
        return IngestionUtils.compressStream(inputStream, false)
                .flatMap(bytes -> {
                    size.add(bytes.available());
                    return blobAsyncClient.getBlockBlobAsyncClient().upload(BinaryData.fromStream(bytes, (long) bytes.available()), true);
                })
                .map(x -> size.getValue());
    }

}
