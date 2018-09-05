package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

class KustoIngestClientImpl implements KustoIngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;

    public KustoIngestClientImpl(KustoConnectionStringBuilder kcsb) {
        log.info("Creating a new KustoIngestClient");
        KustoClient kustoClient = new KustoClient(kcsb);
        resourceManager = new ResourceManager(kustoClient);
    }

    @Override
    public KustoIngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception {
        if (blobSourceInfo == null) {
            throw new KustoClientException("blobs must have at least 1 path");
        }

        ingestionProperties.setAuthorizationContextToken(resourceManager.getKustoIdentityToken());

        List<KustoClientException> ingestionErrors = new LinkedList();
        List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

        try {
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
                status.setIngestionSourcePath(blobSourceInfo.getBlobPath());

                AzureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            postMessageToQueue(resourceManager.getIngestionResource(ResourceManager.ResourceTypes.SECURED_READY_FOR_AGGREGATION_QUEUE), serializedIngestionBlobInfo);
        } catch (Exception ex) {
            ingestionErrors.add(
                    new KustoClientException(blobSourceInfo.getBlobPath(), "fail to post message to queue", ex));
        }

        if (ingestionErrors.size() > 0) {
            throw new KustoClientAggregateException(ingestionErrors);
        }

        return new TableReportKustoIngestionResult(tableStatuses);

    }

    @Override
    public KustoIngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception {
        try {

            String blobName = genBlobName(fileSourceInfo.getFilePath(), ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName, resourceManager.getIngestionResource(ResourceManager.ResourceTypes.TEMP_STORAGE));
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(fileSourceInfo.getFilePath());

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (Exception ex) {
            log.error("ingestFromFile: Error ingesting local file: {}", fileSourceInfo.getFilePath(), ex);
            throw ex;
        }
    }

    @Nullable
    private Long estimateBlobRawSize(@org.jetbrains.annotations.NotNull BlobSourceInfo blobSourceInfo) throws Exception {
        String blobPath = blobSourceInfo.getBlobPath();
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

    private long estimateFileRawSize(String filePath) {
        File file = new File(filePath);
        long fileSize = file.length();
        if (filePath.endsWith(".zip") || filePath.endsWith(".gz")) {
            fileSize = fileSize * COMPRESSED_FILE_MULTIPLIER;
        }
        return fileSize;
    }

    private String genBlobName(String filePath, String databaseName, String tableName) {
        String fileName = (new File(filePath)).getName();
        return String.format("%s__%s__%s__%s", databaseName, tableName, UUID.randomUUID().toString(), fileName);
    }


    // TODO: redesign to avoid those wrapper methods over static ones.
    public void postMessageToQueue(String ingestionResource, String serializedIngestionBlobInfo) throws Exception {
        AzureStorageHelper.postMessageToQueue(ingestionResource, serializedIngestionBlobInfo);
    }

    public CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String ingestionResource) throws Exception {
        return AzureStorageHelper.uploadLocalFileToBlob(filePath, blobName, ingestionResource);
    }


    @Override
    public KustoIngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public KustoIngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception {
        throw new UnsupportedOperationException();
    }


}
