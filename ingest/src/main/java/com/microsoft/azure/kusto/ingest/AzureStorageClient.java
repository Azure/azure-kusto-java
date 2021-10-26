// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

class AzureStorageClient {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int GZIP_BUFFER_SIZE = 16384;
    private static final int STREAM_BUFFER_SIZE = 16384;

    void postMessageToQueue(String queuePath, String content) throws StorageException, URISyntaxException {
        // Ensure
        Ensure.stringIsNotBlank(queuePath, "queuePath");
        Ensure.stringIsNotBlank(content, "content");

        CloudQueue queue = new CloudQueue(new URI(queuePath));
        CloudQueueMessage queueMessage = new CloudQueueMessage(content);
        queue.addMessage(queueMessage);
    }

    void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException,
            URISyntaxException {
        // Ensure
        Ensure.stringIsNotBlank(tableUri, "tableUri");
        Ensure.argIsNotNull(entity, "entity");

        CloudTable table = new CloudTable(new URI(tableUri));
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri, IngestionProperties.DataFormat dataFormat)
            throws URISyntaxException, StorageException, IOException {
        Ensure.fileExists(filePath);

        CompressionType sourceCompressionType = getCompression(filePath);
        return uploadLocalFileToBlob(filePath, blobName, storageUri, shouldCompress(sourceCompressionType, dataFormat.name()));
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri, boolean shouldCompress)
            throws URISyntaxException, StorageException, IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", filePath, blobName, storageUri);

        // Ensure
        Ensure.fileExists(filePath);
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.stringIsNotBlank(storageUri, "storageUri");

        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        if (shouldCompress) {
            compressAndUploadFileToBlob(filePath, blob);
        } else {
            File file = new File(filePath);
            uploadFileToBlob(file, blob);
        }

        return blob;
    }

    void compressAndUploadFileToBlob(String filePath, CloudBlockBlob blob) throws IOException, StorageException {
        // Ensure
        Ensure.fileExists(filePath);
        Ensure.argIsNotNull(blob, "blob");

        try (InputStream fin = Files.newInputStream(Paths.get(filePath));
             GZIPOutputStream gzout = new GZIPOutputStream(blob.openOutputStream())) {
            copyStream(fin, gzout, GZIP_BUFFER_SIZE);
        }
    }

    void uploadFileToBlob(File sourceFile, CloudBlockBlob blob) throws IOException, StorageException {
        // Ensure
        Ensure.argIsNotNull(blob, "blob");
        Ensure.fileExists(sourceFile, "sourceFile");

        blob.uploadFromFile(sourceFile.getAbsolutePath());
    }

    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean shouldCompress)
            throws IOException, URISyntaxException, StorageException {
        log.debug("uploadStreamToBlob: blobName: {}, storageUri: {}", blobName, storageUri);

        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.stringIsNotBlank(blobName, "blobName");
        Ensure.stringIsNotBlank(storageUri, "storageUri");

        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        if (shouldCompress) {
            compressAndUploadStream(inputStream, blob);
        } else {
            uploadStream(inputStream, blob);
        }
        return blob;
    }

    void uploadStream(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        BlobOutputStream bos = blob.openOutputStream();
        copyStream(inputStream, bos, STREAM_BUFFER_SIZE);
        bos.close();
    }

    void compressAndUploadStream(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Ensure
        Ensure.argIsNotNull(inputStream, "inputStream");
        Ensure.argIsNotNull(blob, "blob");

        try (
             GZIPOutputStream gzout = new GZIPOutputStream(blob.openOutputStream())) {
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

    String getBlobPathWithSas(CloudBlockBlob blob) {
        Ensure.argIsNotNull(blob, "blob");

        StorageCredentialsSharedAccessSignature signature =
                (StorageCredentialsSharedAccessSignature) blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }

    long getBlobSize(String blobPath) throws StorageException, URISyntaxException {
        Ensure.stringIsNotBlank(blobPath, "blobPath");

        CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
        blockBlob.downloadAttributes();
        return blockBlob.getProperties().getLength();
    }

    static CompressionType getCompression(String fileName) {
        if (fileName.endsWith(".gz")) {
            return CompressionType.gz;
        }
        if (fileName.endsWith(".zip")) {
            return CompressionType.zip;
        }
        return null;
    }

    // We don't support compression of Parquet and Orc files
    static boolean shouldCompress(CompressionType sourceCompressionType, String dataFormat) {
        return sourceCompressionType == null
                && (dataFormat == null ||
                (!dataFormat.equals(IngestionProperties.DataFormat.parquet.name())
                        && !dataFormat.equals(IngestionProperties.DataFormat.orc.name())));
    }

    static String removeExtension(String filename) {
        if (filename == null) {
            return null;
        }

        int extensionPos = filename.lastIndexOf('.');
        int lastDirSeparator = filename.lastIndexOf('\\');
        if (lastDirSeparator > extensionPos) {
            return filename;
        } else {
            return filename.substring(0, extensionPos);
        }
    }
}