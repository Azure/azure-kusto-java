package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class IngestClientImpl implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;
    private AzureStorageHelper azureStorageHelper;

    IngestClientImpl(ResourceManager resourceManager, AzureStorageHelper azureStorageHelper){
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        this.azureStorageHelper = azureStorageHelper;
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {

        // Argument validation:
        if (blobSourceInfo == null){
            throw new IllegalArgumentException("blobSourceInfo is null");
        }
        if (ingestionProperties == null){
            throw new IllegalArgumentException("ingestionProperties is null");
        }
        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            ingestionProperties.setAuthorizationContextToken(resourceManager.getIdentityToken());
            List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

            // Create the ingestion message
            IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobSourceInfo.getBlobPath(),
                    ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            ingestionBlobInfo.rawDataSize = blobSourceInfo.getRawSizeInBytes() > 0L ? blobSourceInfo.getRawSizeInBytes()
                    : estimateBlobRawSize(blobSourceInfo.getBlobPath());
            ingestionBlobInfo.reportLevel = ingestionProperties.getReportLevel();
            ingestionBlobInfo.reportMethod = ingestionProperties.getReportMethod();
            ingestionBlobInfo.flushImmediately = ingestionProperties.getFlushImmediately();
            ingestionBlobInfo.additionalProperties = ingestionProperties.getAdditionalProperties();
            if (blobSourceInfo.getSourceId() != null) {
                ingestionBlobInfo.id = blobSourceInfo.getSourceId();
            }

            if (ingestionProperties.getReportMethod() != IngestionProperties.IngestionReportMethod.Queue) {
                String tableStatusUri = resourceManager.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE);
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
                status.setIngestionSourcePath(blobSourceInfo.getBlobPath());

                azureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            azureStorageHelper.postMessageToQueue(
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);

            return new TableReportIngestionResult(tableStatuses);

        } catch (StorageException e) {
            throw new IngestionServiceException("Failed to ingest from blob", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from blob", e);
        }
    }

    @Override
    public CompletableFuture<IngestionResult> ingestFromBlobAsync(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return ingestFromBlob(blobSourceInfo, ingestionProperties);
                    } catch (IngestionClientException | IngestionServiceException e) {
                        log.error("Failed to ingest from blob (async)", e);
                        // Here we throw a CompletionException which extends the RuntimeException.
                        // the real exception itself would be in the <cause> of this CompletionException
                        throw new CompletionException(e);
                    }
                });
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        if (fileSourceInfo == null){
            throw new IllegalArgumentException("fileSourceInfo is null");
        }
        if (ingestionProperties == null){
            throw new IllegalArgumentException("ingestionProperties is null");
        }
        fileSourceInfo.validate();
        ingestionProperties.validate();

        String filePath = fileSourceInfo.getFilePath();
        if(!(new File(filePath).exists())){
            throw new IllegalArgumentException("The file does not exist: " + filePath);
        }

        try {
            String fileName = (new File(filePath)).getName();
            String blobName = genBlobName(fileName, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageHelper.uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName, resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE));
            String blobPath = azureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(filePath);

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (StorageException e) {
            throw new IngestionServiceException("Failed to ingest from file", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from file", e);
        }
    }

    @Override
    public CompletableFuture<IngestionResult> ingestFromFileAsync(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return ingestFromFile(fileSourceInfo, ingestionProperties);
                    } catch (IngestionClientException | IngestionServiceException e) {
                        log.error("Failed to ingest from file (async)", e);
                        throw new CompletionException(e);
                    }
                });
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        if (streamSourceInfo == null){
            throw new IllegalArgumentException("streamSourceInfo is null");
        }
        if (ingestionProperties == null){
            throw new IllegalArgumentException("ingestionProperties is null");
        }

        streamSourceInfo.validate();
        ingestionProperties.validate();

        try {
            IngestionResult ingestionResult;
            if (streamSourceInfo.getStream() == null || streamSourceInfo.getStream().available() <= 0) {
                throw new IngestionClientException("Stream is empty");
            }
            String blobName = genBlobName("StreamUpload", ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageHelper.uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE),
                    true
            );
            String blobPath = azureStorageHelper.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, 0); // TODO: check if we can get the rawDataSize locally

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;

        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from stream", e);
        } catch (StorageException e) {
            throw new IngestionServiceException("Failed to ingest from stream", e);
        }
    }

    @Override
    public CompletableFuture<IngestionResult> ingestFromStreamAsync(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return ingestFromStream(streamSourceInfo, ingestionProperties);
                    } catch (IngestionClientException | IngestionServiceException e) {
                        log.error("Failed to ingest from stream (async)", e);
                        throw new CompletionException(e);
                    }
                });
    }

    private long estimateBlobRawSize(String blobPath) throws StorageException, URISyntaxException {
        long blobSize = azureStorageHelper.getBlobSize(blobPath);

        return blobPath.contains(".zip") || blobPath.contains(".gz") ?
                blobSize * COMPRESSED_FILE_MULTIPLIER : blobSize;
    }

    private long estimateFileRawSize(String filePath) {
        File file = new File(filePath);
        long fileSize = file.length();

        return filePath.contains(".zip") || filePath.contains(".gz") ?
                fileSize * COMPRESSED_FILE_MULTIPLIER : fileSize;
    }

    private String genBlobName(String fileName, String databaseName, String tableName) {
        return String.format("%s__%s__%s__%s", databaseName, tableName, UUID.randomUUID().toString(), fileName);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        throw new UnsupportedOperationException();
    }
}
