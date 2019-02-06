package com.microsoft.azure.kusto.ingest;

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

import static com.microsoft.azure.kusto.ingest.Validation.*;

class AzureStorageClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int GZIP_BUFFER_SIZE = 16384;
    private static final int STREAM_BUFFER_SIZE = 16384;

    void postMessageToQueue(String queuePath, String content) throws StorageException {
        // Validation
        validateIsNotBlank(content, "content is empty");
        URI queueUri = validateUri(queuePath);

        CloudQueue queue = new CloudQueue(queueUri);
        CloudQueueMessage queueMessage = new CloudQueueMessage(content);
        queue.addMessage(queueMessage);
    }

    void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException {
        // Validation
        validateIsNotNull(entity, "entity is null");
        URI tableUriObj = validateUri(tableUri);

        CloudTable table = new CloudTable(tableUriObj);
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri)
            throws URISyntaxException, StorageException, IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", filePath, blobName, storageUri);

        // Validation
        validateIsNotBlank(blobName, "blobName is empty");
        URI storageUriObj = validateUri(storageUri);
        File sourceFile = validateFileExists(filePath);

        // Check if the file is already compressed:
        boolean isCompressed = isCompressed(filePath);

        CloudBlobContainer container = new CloudBlobContainer(storageUriObj);
        CloudBlockBlob blob = container.getBlockBlobReference(blobName + (isCompressed ? "" : ".gz"));

        if (!isCompressed) {
            compressAndUploadFileToBlob(filePath, blob);
        } else {
            uploadFileToBlob(sourceFile, blob);
        }

        return blob;
    }

    void compressAndUploadFileToBlob(String filePath, CloudBlockBlob blob) throws IOException, StorageException {
        // Validation
        validateFileExists(filePath);
        validateIsNotNull(blob, "blob is null");

        InputStream fin = Files.newInputStream(Paths.get(filePath));
        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);

        copyStream(fin, gzout, GZIP_BUFFER_SIZE);

        gzout.close();
        fin.close();
    }

    void uploadFileToBlob(File sourceFile, CloudBlockBlob blob) throws IOException, StorageException {
        // Validation
        validateIsNotNull(blob, "blob is null");
        validateFileExists(sourceFile, "The sourceFile does not exist");

        blob.uploadFromFile(sourceFile.getAbsolutePath());
    }

    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean compress)
            throws IOException, URISyntaxException, StorageException {
        log.debug("uploadStreamToBlob: blobName: {}, storageUri: {}", blobName, storageUri);

        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotBlank(blobName, "blobName is empty");
        URI storageUriObj = validateUri(storageUri);

        CloudBlobContainer container = new CloudBlobContainer(storageUriObj);
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        if (compress) {
            compressAndUploadStream(inputStream, blob);
        } else {
            uploadStream(inputStream, blob);
        }
        return blob;
    }

    void uploadStream(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotNull(blob, "blob is null");

        BlobOutputStream bos = blob.openOutputStream();
        copyStream(inputStream, bos, STREAM_BUFFER_SIZE);
        bos.close();
    }

    void compressAndUploadStream(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotNull(blob, "blob is null");

        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);
        copyStream(inputStream, gzout, GZIP_BUFFER_SIZE);
        gzout.close();
    }

    private void copyStream(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }
    }

    String getBlobPathWithSas(CloudBlockBlob blob) {
        validateIsNotNull(blob, "blob is null");

        StorageCredentialsSharedAccessSignature signature =
                (StorageCredentialsSharedAccessSignature) blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }

    long getBlobSize(String blobPath) throws StorageException {
        URI blobUri = validateUri(blobPath);

        CloudBlockBlob blockBlob = new CloudBlockBlob(blobUri);
        blockBlob.downloadAttributes();
        return blockBlob.getProperties().getLength();
    }

    boolean isCompressed(String fileName) {
        return fileName.endsWith(".gz") || fileName.endsWith(".zip");
    }
}
