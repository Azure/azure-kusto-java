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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

class AzureStorageHelper {

    private static final Logger log = LoggerFactory.getLogger(AzureStorageHelper.class);
    private static final int GZIP_BUFFER_SIZE = 16384;
    private static final int STREAM_BUFFER_SIZE = 16384;

    void postMessageToQueue(String queuePath, String content) throws StorageException, URISyntaxException {
        // Validation
        validateIsNotEmpty(queuePath, "queuePath is empty");
        validateIsNotEmpty(content, "content is empty");

        CloudQueue queue = new CloudQueue(new URI(queuePath));
        CloudQueueMessage queueMessage = new CloudQueueMessage(content);
        queue.addMessage(queueMessage);
    }

    void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException, URISyntaxException {
        // Validation
        validateIsNotEmpty(tableUri, "tableUri is empty");
        validateIsNotNull(entity, "entity is null");

        CloudTable table = new CloudTable(new URI(tableUri));
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws URISyntaxException, StorageException, IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", filePath, blobName, storageUri);

        // Validation
        validateIsNotEmpty(filePath, "filePath is empty");
        validateIsNotEmpty(blobName, "blobName is empty");
        validateIsNotEmpty(storageUri, "storageUri is empty");

        File sourceFile = new File(filePath);
        validateFileExists(sourceFile, "The file does not exist: " + filePath);

        // Check if the file is already compressed:
        boolean isCompressed = filePath.endsWith(".gz") || filePath.endsWith(".zip");

        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        CloudBlockBlob blob = container.getBlockBlobReference(blobName + (isCompressed?"":".gz"));

        if(!isCompressed){
            compressAndUploadFileToBlob(filePath, blob);
        } else {
            uploadFileToBlob(sourceFile, blob);
        }

        return blob;
    }

    void compressAndUploadFileToBlob(String filePath, CloudBlockBlob blob) throws IOException, StorageException {
        // Validation
        validateIsNotEmpty(filePath,"filePath is empty");
        validateIsNotNull(blob, "blob is null");

        InputStream fin = Files.newInputStream(Paths.get(filePath));
        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);

        stream(fin, gzout, GZIP_BUFFER_SIZE);

        gzout.close();
        fin.close();
    }

    void uploadFileToBlob(File sourceFile, CloudBlockBlob blob) throws IOException, StorageException {
        // Validation
        validateIsNotNull(blob, "blob is null");
        validateIsNotNull(sourceFile, "sourceFile is null");
        validateFileExists(sourceFile, "The file does not exist");

        blob.uploadFromFile(sourceFile.getAbsolutePath());
    }

    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean compress) throws IOException, URISyntaxException, StorageException {
        log.debug("uploadLocalFileToBlob: blobName: {}, storageUri: {}", blobName, storageUri);

        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotEmpty(blobName, "blobName is empty");
        validateIsNotEmpty(storageUri, "storageUri is empty");

        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        if(compress){
            compressAndStreamToBlob(inputStream, blob);
        }else{
            streamToBlob(inputStream, blob);
        }
        return blob;
    }

    void streamToBlob(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotNull(blob, "blob is null");

        BlobOutputStream bos = blob.openOutputStream();
        stream(inputStream,bos,STREAM_BUFFER_SIZE);
        bos.close();
    }

    void compressAndStreamToBlob(InputStream inputStream, CloudBlockBlob blob) throws StorageException, IOException {
        // Validation
        validateIsNotNull(inputStream, "inputStream is null");
        validateIsNotNull(blob, "blob is null");

        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);
        stream(inputStream,gzout,GZIP_BUFFER_SIZE);
        gzout.close();
    }

    private void stream(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }
    }

    String getBlobPathWithSas(CloudBlockBlob blob) {
        validateIsNotNull(blob, "blob is null");

        StorageCredentialsSharedAccessSignature signature = (StorageCredentialsSharedAccessSignature)blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }

    long getBlobSize(String blobPath) throws StorageException, URISyntaxException {
        validateIsNotEmpty(blobPath, "blobPath is empty");

        CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
        blockBlob.downloadAttributes();
        return blockBlob.getProperties().getLength();
    }

    // Validation functions:
    private void validateIsNotEmpty(String str, String message) {
        if(StringUtils.isEmpty(str)){
            throw new IllegalArgumentException(message);
        }
    }

    private void validateIsNotNull(Object obj, String message) {
        if(obj == null){
            throw new IllegalArgumentException(message);
        }
    }

    private void validateFileExists(File file, String message) {
        if(!file.exists()){
            throw new IllegalArgumentException(message);
        }
    }

}
