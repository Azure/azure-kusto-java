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
        CloudQueue queue = new CloudQueue(new URI(queuePath));
        CloudQueueMessage queueMessage = new CloudQueueMessage(content);
        queue.addMessage(queueMessage);
    }

    void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException, URISyntaxException {
        CloudTable table = new CloudTable(new URI(tableUri));
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws URISyntaxException, StorageException, IOException {
        log.debug("uploadLocalFileToBlob: filePath: {}, blobName: {}, storageUri: {}", filePath, blobName, storageUri);

        // Check if the file is already compressed:
        boolean isCompressed = filePath.endsWith(".gz") || filePath.endsWith(".zip");

        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        File sourceFile = new File(filePath);

        CloudBlockBlob blob = container.getBlockBlobReference(blobName + (isCompressed ? "" : ".gz"));

        if (!isCompressed) {
            compressAndUploadFile(filePath, blob);
        } else {
            blob.uploadFromFile(sourceFile.getAbsolutePath());
        }

        return blob;
    }

    private void compressAndUploadFile(String filePath, CloudBlockBlob blob) throws IOException, StorageException {
        InputStream fin = Files.newInputStream(Paths.get(filePath));
        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);

        copyStream(fin, gzout, GZIP_BUFFER_SIZE);

        gzout.close();
        fin.close();
    }

    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean compress) throws IOException, URISyntaxException, StorageException {
        log.debug("uploadStreamToBlob - blobName: {}, storageUri: {}", blobName, storageUri);
        CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
        CloudBlockBlob blob = container.getBlockBlobReference(blobName + (compress ? ".gz" : ""));
        BlobOutputStream bos = blob.openOutputStream();
        if (compress) {
            GZIPOutputStream gzout = new GZIPOutputStream(bos);
            copyStream(inputStream, gzout, GZIP_BUFFER_SIZE);
            gzout.close();
        } else {
            copyStream(inputStream, bos, STREAM_BUFFER_SIZE);
            bos.close();
        }
        return blob;
    }

    private void copyStream(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int length;
        while ((length = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, length);
        }
    }

    String getBlobPathWithSas(CloudBlockBlob blob) {
        StorageCredentialsSharedAccessSignature signature = (StorageCredentialsSharedAccessSignature) blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }

}
