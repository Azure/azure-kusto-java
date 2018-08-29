package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class KustoIngestClient {
    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final Logger log = LoggerFactory.getLogger(KustoIngestClient.class);
    private ResourceManager resourceManager;

    public KustoIngestClient(KustoConnectionStringBuilder kcsb) {
        log.info("Creating a new KustoIngestClient");
        KustoClient kustoClient = new KustoClient(kcsb);
        resourceManager = new ResourceManager(kustoClient);
    }

    public IKustoIngestionResult ingestFromMultipleBlobsPaths(List<String> blobPaths, Boolean deleteSourceOnSuccess,
                                                              KustoIngestionProperties ingestionProperties) throws Exception {

        List<BlobDescription> blobDescriptions = blobPaths.stream().map(b -> new BlobDescription(b, null))
                .collect(Collectors.toList());
        return ingestFromMultipleBlobs(blobDescriptions, deleteSourceOnSuccess, ingestionProperties);
    }

    public IKustoIngestionResult ingestFromSingleBlob(String blobPath, Boolean deleteSourceOnSuccess,
                                                      KustoIngestionProperties ingestionProperties, Long rawDataSize) throws Exception {

        BlobDescription blobDescription = new BlobDescription(blobPath, rawDataSize);
        return ingestFromMultipleBlobs(new ArrayList(Arrays.asList(blobDescription)), deleteSourceOnSuccess, ingestionProperties);
    }

    public IKustoIngestionResult ingestFromMultipleBlobs(List<BlobDescription> blobDescriptions,
                                                         Boolean deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties) throws Exception {
        if (blobDescriptions == null || blobDescriptions.size() == 0) {
            throw new KustoClientException("blobs must have at least 1 path");
        }

        ingestionProperties.setAuthorizationContextToken(resourceManager.getKustoIdentityToken());

        List<KustoClientException> ingestionErrors = new LinkedList();
        List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

        for (BlobDescription blobDescription : blobDescriptions) {
            try {
                // Create the ingestion message
                IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobDescription.getBlobPath(),
                        ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
                ingestionBlobInfo.rawDataSize = blobDescription.getBlobSize() != null ? blobDescription.getBlobSize()
                        : estimateBlobRawSize(blobDescription);
                ingestionBlobInfo.retainBlobOnSuccess = !deleteSourceOnSuccess;
                ingestionBlobInfo.reportLevel = ingestionProperties.getReportLevel();
                ingestionBlobInfo.reportMethod = ingestionProperties.getReportMethod();
                ingestionBlobInfo.flushImmediately = ingestionProperties.getFlushImmediately();
                ingestionBlobInfo.additionalProperties = ingestionProperties.getAdditionalProperties();
                if (blobDescription.getSourceId() != null) {
                    ingestionBlobInfo.id = blobDescription.getSourceId();
                }

                if (ingestionProperties.getReportMethod() != KustoIngestionProperties.IngestionReportMethod.Queue) {
                    String tableStatusUri = resourceManager.getIngestionResource(ResourceManager.ResourceTypes.INGESTIONS_STATUS_TABLE);
                    ingestionBlobInfo.IngestionStatusInTable = new IngestionStatusInTableDescription();
                    ingestionBlobInfo.IngestionStatusInTable.TableConnectionString = tableStatusUri;
                    ingestionBlobInfo.IngestionStatusInTable.RowKey = ingestionBlobInfo.id.toString();
                    ingestionBlobInfo.IngestionStatusInTable.PartitionKey = ingestionBlobInfo.id.toString();

                    IngestionStatus status = new IngestionStatus(ingestionBlobInfo.id);
                    status.database = ingestionProperties.getDatabaseName();
                    status.table = ingestionProperties.getTableName();
                    status.status = OperationStatus.Pending;
                    status.updatedOn = Date.from(Instant.now());
                    status.ingestionSourceId = ingestionBlobInfo.id;
                    status.setIngestionSourcePath(blobDescription.getBlobPath());

                    AzureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                    tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
                }

                ObjectMapper objectMapper = new ObjectMapper();
                String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

                postMessageToQueue(resourceManager.getIngestionResource(ResourceManager.ResourceTypes.SECURED_READY_FOR_AGGREGATION_QUEUE), serializedIngestionBlobInfo);
            } catch (Exception ex) {
                ingestionErrors.add(
                        new KustoClientException(blobDescription.getBlobPath(), "fail to post message to queue", ex));
            }
        }

        if (ingestionErrors.size() > 0) {
            throw new KustoClientAggregateException(ingestionErrors);
        }

        return new TableReportKustoIngestionResult(tableStatuses);
    }

    public void postMessageToQueue(String ingestionResource, String serializedIngestionBlobInfo) throws Exception {
        AzureStorageHelper.postMessageToQueue(ingestionResource,serializedIngestionBlobInfo);
    }

    public void ingestFromSingleFile(String filePath, KustoIngestionProperties ingestionProperties) throws Exception {
        try {
            String blobName = genBlobName(filePath, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = uploadLocalFileToBlob(filePath,blobName, resourceManager.getIngestionResource(ResourceManager.ResourceTypes.TEMP_STORAGE));
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = estimateLocalFileSize(filePath);

            ingestFromSingleBlob(blobPath, true, ingestionProperties, rawDataSize);

        } catch (Exception ex) {
            log.error(String.format("ingestFromSingleFile: Error while uploading file (compression mode): %s. Error: %s", filePath, ex.getMessage()), ex);
            throw ex;
        }
    }

    public CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String ingestionResource) throws Exception {
        return AzureStorageHelper.uploadLocalFileToBlob(filePath,blobName,ingestionResource);
    }

    private Long estimateBlobRawSize(BlobDescription blobDescription) throws Exception {
        String blobPath = blobDescription.getBlobPath();
        CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
        blockBlob.downloadAttributes();
        long length = blockBlob.getProperties().getLength();

        if (length == 0) {
            return null;
        }
        if (blobPath.contains(".zip") || blobPath.contains(".gz")) {
            length = length * COMPRESSED_FILE_MULTIPLIER;
        }

        return length;
    }

    private long estimateLocalFileSize(String filePath){
        File file = new File(filePath);
        long fileSize = file.length();
        if(filePath.endsWith(".zip") || filePath.endsWith(".gz")){
            fileSize = fileSize * COMPRESSED_FILE_MULTIPLIER;
        }
        return fileSize;
    }

    private String genBlobName(String filePath, String databaseName, String tableName) {
        String fileName = (new File(filePath)).getName();
        return String.format("%s__%s__%s__%s",databaseName,tableName,UUID.randomUUID().toString(),fileName);
    }

    private List<IngestionBlobInfo> GetAndDiscardTopIngestionFailures() throws Exception {
        // Get ingestion queues from DM
//        KustoResults failedIngestionsQueues = kustoClient
//                .execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
//        String failedIngestionsQueue = failedIngestionsQueues.getValues().get(0)
//                .get(failedIngestionsQueues.getIndexByColumnName("Uri"));
        String failedIngestionsQueue = resourceManager.getIngestionResource(ResourceManager.ResourceTypes.FAILED_INGESTIONS_QUEUE);
        CloudQueue queue = new CloudQueue(new URI(failedIngestionsQueue));
        Iterable<CloudQueueMessage> messages = queue.retrieveMessages(32, 5000, null, null);
        return null; // Will be implemented in future
    }
}
