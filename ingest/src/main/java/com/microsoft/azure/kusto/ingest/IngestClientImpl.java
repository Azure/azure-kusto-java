package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

class IngestClientImpl implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;

    public IngestClientImpl(KustoConnectionStringBuilder kcsb) throws Exception {
        log.info("Creating a new IngestClient");
        KustoClient kustoClient = new KustoClient(kcsb);
        resourceManager = new ResourceManager(kustoClient);
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws Exception {
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

            if (ingestionProperties.getReportMethod() != IngestionProperties.IngestionReportMethod.Queue) {
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

            postMessageToQueue(
                    resourceManager.getIngestionResource(ResourceManager.ResourceTypes.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);

        } catch (Exception ex) {
            ingestionErrors.add(
                    new KustoClientException(blobSourceInfo.getBlobPath(), "fail to post message to queue", ex));
        }

        if (ingestionErrors.size() > 0) {
            throw new KustoClientAggregateException(ingestionErrors);
        }

        return new TableReportIngestionResult(tableStatuses);
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws Exception {
        try {
            String fileName = (new File(fileSourceInfo.getFilePath())).getName();
            String blobName = genBlobName(fileName, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
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

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws Exception {
        try {
            IngestionResult ingestionResult;
            if (streamSourceInfo.getStream() == null || streamSourceInfo.getStream().available() <= 0) {
                throw new KustoClientException("stream is empty");
            }
            String blobName = genBlobName("StreamUpload", ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceTypes.TEMP_STORAGE),
                    true
            );
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, 0); // TODO: check if we can get the rawDataSize locally

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;
        } catch (Exception ex) {
            log.error(String.format("ingestFromStream: Error while ingesting from stream. Error: %s", ex.getMessage()), ex);
            throw ex;
        }
    }

    @Override
    public IngestionResult ingestFromByteArray(ByteArraySourceInfo byteArraySourceInfo, IngestionProperties ingestionProperties) throws Exception {
        try {
            String fileName = byteArraySourceInfo.getName();
            String blobName = genBlobName(fileName, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            int rawDataSize = byteArraySourceInfo.getSize();

            CloudBlockBlob blob = uploadByteArrayToBlob(
                    byteArraySourceInfo.getByteArray(),
                    rawDataSize,
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceTypes.TEMP_STORAGE));

            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, byteArraySourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (Exception ex) {
            log.error("ingestFromByteArray: Error ingesting local byte array: {}", byteArraySourceInfo.getName(), ex);
            throw ex;
        }
    }

    private Long estimateBlobRawSize(@org.jetbrains.annotations.NotNull BlobSourceInfo blobSourceInfo) throws Exception {
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
    void postMessageToQueue(String queuePath, String serializedIngestionBlobInfo) throws Exception {
        AzureStorageHelper.postMessageToQueue(queuePath, serializedIngestionBlobInfo);
    }

    // TODO: redesign to avoid those wrapper methods over static ones.
    CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws Exception {
        return AzureStorageHelper.uploadLocalFileToBlob(filePath, blobName, storageUri);
    }

    // TODO: redesign to avoid those wrapper methods over static ones.
    CloudBlockBlob uploadStreamToBlob(InputStream inputStream, String blobName, String storageUri, boolean compress) throws Exception {
        return AzureStorageHelper.uploadStreamToBlob(inputStream, blobName, storageUri, compress);
    }

    // TODO: redesign to avoid those wrapper methods over static ones.
    CloudBlockBlob uploadByteArrayToBlob(byte[] bytes, int size, String blobName, String storageUri) throws Exception {
        return AzureStorageHelper.uploadByteArrayToBlob(bytes, size, blobName, storageUri);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws Exception {
        throw new UnsupportedOperationException();
    }
}
