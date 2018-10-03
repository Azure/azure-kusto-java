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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

class IngestClientImpl implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;

    public IngestClientImpl(ConnectionStringBuilder csb) {
        log.info("Creating a new IngestClient");
        Client client = ClientFactory.createClient(csb);
        resourceManager = new ResourceManager(client);
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        if (blobSourceInfo == null) {
            throw new IngestionClientException("blobSourceInfo is null");
        }
        if (blobSourceInfo.getBlobPath() == null || blobSourceInfo.getBlobPath().isEmpty()) {
            throw new IngestionClientException("blobPath is empty");
        }

        try {
            ingestionProperties.setAuthorizationContextToken(resourceManager.getIdentityToken());
            List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

            // Create the ingestion message
            IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobSourceInfo.getBlobPath(),
                    ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            ingestionBlobInfo.rawDataSize = blobSourceInfo.getRawSizeInBytes() > 0L ? blobSourceInfo.getRawSizeInBytes()
                    : estimateBlobRawSize(blobSourceInfo);
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

                AzureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            postMessageToQueue(
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);
            return new TableReportIngestionResult(tableStatuses);

        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromBlob()", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromBlob()", e);
        }
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        try {
            String fileName = (new File(fileSourceInfo.getFilePath())).getName();
            String blobName = genBlobName(fileName, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName, resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE));
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(fileSourceInfo.getFilePath());

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromFile()", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromFile()", e);
        }
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        try {
            IngestionResult ingestionResult;
            if (streamSourceInfo.getStream() == null || streamSourceInfo.getStream().available() <= 0) {
                throw new IngestionClientException("Stream is empty");
            }
            String blobName = genBlobName("StreamUpload", ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE),
                    true
            );
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, 0); // TODO: check if we can get the rawDataSize locally

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromStream()", e);
        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromStream()", e);
        }
    }

    private Long estimateBlobRawSize(@org.jetbrains.annotations.NotNull BlobSourceInfo blobSourceInfo) throws IngestionClientException {
        try {
            String blobPath = blobSourceInfo.getBlobPath();
            CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
            blockBlob.downloadAttributes();
            long length = blockBlob.getProperties().getLength();

            if (length == 0) {
                return length;
            }

            if (blobPath.contains(".zip") || blobPath.contains(".gz")) {
                length = length * COMPRESSED_FILE_MULTIPLIER;
            }

            return length;
        } catch (StorageException | URISyntaxException e) {
            throw new IngestionClientException("Error in estimateBlobRawSize", e);
        }
    }

    private long estimateFileRawSize(String filePath) {
        File file = new File(filePath);
        long fileSize = file.length();
        if (filePath.endsWith(".zip") || filePath.endsWith(".gz")) {
            fileSize = fileSize * COMPRESSED_FILE_MULTIPLIER;
        }
        return fileSize;
    }

    private String genBlobName(String fileName, String databaseName, String tableName) {
        return String.format("%s__%s__%s__%s", databaseName, tableName, UUID.randomUUID().toString(), fileName);
    }


    // TODO: redesign to avoid those wrapper methods over static ones.
    void postMessageToQueue(String queuePath, String serializedIngestionBlobInfo) throws URISyntaxException, StorageException {
        AzureStorageHelper.postMessageToQueue(queuePath, serializedIngestionBlobInfo);
    }

    // TODO: redesign to avoid those wrapper methods over static ones.
    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws StorageException, IOException, URISyntaxException {
        return AzureStorageHelper.uploadLocalFileToBlob(filePath, blobName, storageUri);
    }

    // TODO: redesign to avoid those wrapper methods over static ones.
    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean compress) throws StorageException, IOException, URISyntaxException {
        return AzureStorageHelper.uploadStreamToBlob(inputStream, blobName, storageUri, compress);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        throw new UnsupportedOperationException();
    }
}
