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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

class AzureStorageHelper {

    private static final Logger log = LoggerFactory.getLogger(AzureStorageHelper.class);
    private static final int GZIP_BUFFER_SIZE = 16384;

    public static void postMessageToQueue(String queuePath, String content) throws Exception {

        try
        {
            CloudQueue queue = new CloudQueue(new URI(queuePath));
            CloudQueueMessage queueMessage = new CloudQueueMessage(content);
            queue.addMessage(queueMessage);
        }
        catch (Exception e)
        {
            log.error(String.format("postMessageToQueue: %s.",e.getMessage()), e);
            throw e;
        }
    }

    public static void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException, URISyntaxException {
        CloudTable table = new CloudTable(new URI(tableUri));
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    public static CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws Exception{
        try {
            log.debug(String.format("uploadLocalFileToBlob: filePath: %s, blobName: %s, storageUri: %s", filePath, blobName, storageUri));

            // Check if the file is already compressed:
            boolean isCompressed = filePath.endsWith(".gz") || filePath.endsWith(".zip");

            CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
            File sourceFile = new File(filePath);

            CloudBlockBlob blob = container.getBlockBlobReference(blobName + (isCompressed?"":".gz"));

            if(!isCompressed){
                compressAndUploadFile(filePath, blob);
            } else {
                blob.uploadFromFile(sourceFile.getAbsolutePath());
            }

            return blob;
        }
        catch (StorageException se)
        {
            log.error(String.format("uploadLocalFileToBlob: Error returned from the service. Http code: %d and error code: %s", se.getHttpStatusCode(), se.getErrorCode()), se);
            throw se;
        }

        catch (Exception ex)
        {
            log.error(String.format("uploadLocalFileToBlob: Error while uploading file to blob."), ex);
            throw ex;
        }
    }

    private static void compressAndUploadFile(String filePath, CloudBlockBlob blob) throws IOException, StorageException {
        InputStream fin = Files.newInputStream(Paths.get(filePath));
        BlobOutputStream bos = blob.openOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(bos);

        byte[] buffer = new byte[GZIP_BUFFER_SIZE];
        int length;
        while ((length = fin.read(buffer)) > 0) {
            gzout.write(buffer, 0, length);
        }

        gzout.close();
        fin.close();
    }

    public static String getBlobPathWithSas(CloudBlockBlob blob) {
        StorageCredentialsSharedAccessSignature signature = (StorageCredentialsSharedAccessSignature)blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }
}
