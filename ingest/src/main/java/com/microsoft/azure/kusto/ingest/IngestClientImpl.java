package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.univocity.parsers.csv.CsvRoutines;

import java.io.*;
import java.lang.invoke.MethodHandles;
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
    private AzureStorageClient azureStorageClient;

    IngestClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        Client client = ClientFactory.createClient(csb);
        this.resourceManager = new ResourceManager(client);
        this.azureStorageClient = new AzureStorageClient();
    }

    IngestClientImpl(ResourceManager resourceManager) {
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        azureStorageClient = new AzureStorageClient();
    }

    IngestClientImpl(ResourceManager resourceManager, AzureStorageClient azureStorageClient) {
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        this.azureStorageClient = azureStorageClient;
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {

        // Argument validation:
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            ingestionProperties.setAuthorizationContextToken(resourceManager.getIdentityToken());
            List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

            // Create the ingestion message
            IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobSourceInfo.getBlobPath(),
                    ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            String urlWithoutSecrets = SecurityUtils.removeSecretsFromUrl(blobSourceInfo.getBlobPath());
            if (blobSourceInfo.getRawSizeInBytes() > 0L) {
                ingestionBlobInfo.rawDataSize = blobSourceInfo.getRawSizeInBytes();
            } else {
                log.warn("Blob '{}' was sent for ingestion without specifying its raw data size", urlWithoutSecrets);
            }

            ingestionBlobInfo.reportLevel = ingestionProperties.getReportLevel();
            ingestionBlobInfo.reportMethod = ingestionProperties.getReportMethod();
            ingestionBlobInfo.flushImmediately = ingestionProperties.getFlushImmediately();
            ingestionBlobInfo.additionalProperties = ingestionProperties.getIngestionProperties();
            if (blobSourceInfo.getSourceId() != null) {
                ingestionBlobInfo.id = blobSourceInfo.getSourceId();
            }

            if (ingestionProperties.getReportMethod() != IngestionProperties.IngestionReportMethod.Queue) {
                String tableStatusUri = resourceManager
                        .getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE);
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
                status.setIngestionSourcePath(urlWithoutSecrets);

                azureStorageClient.azureTableInsertEntity(tableStatusUri, status);
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            azureStorageClient.postMessageToQueue(
                    resourceManager
                            .getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);
            return new TableReportIngestionResult(tableStatuses);

        } catch (StorageException e) {
            throw new IngestionServiceException("Failed to ingest from blob", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from blob", e);
        }
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            String filePath = fileSourceInfo.getFilePath();
            Ensure.fileExists(filePath);
            CompressionType sourceCompressionType = azureStorageClient.getCompression(filePath);
            boolean shouldCompress = azureStorageClient.shouldCompress(sourceCompressionType, ingestionProperties.getDataFormat());

            File file = new File(filePath);
            String blobName = genBlobName(
                    file.getName(),
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    ingestionProperties.getDataFormat(),
                    shouldCompress ? CompressionType.gz : sourceCompressionType);

            CloudBlockBlob blob = azureStorageClient.uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE), shouldCompress);
            String blobPath = azureStorageClient.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(filePath, IngestionProperties.DATA_FORMAT.valueOf(ingestionProperties.getDataFormat()));

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (StorageException e) {
            throw new IngestionServiceException("Failed to ingest from file", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from file", e);
        }
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        try {
            IngestionResult ingestionResult;
            if (streamSourceInfo.getStream() == null || streamSourceInfo.getStream().available() <= 0) {
                throw new IngestionClientException("Stream");
            }

            String blobName = genBlobName(
                    "StreamUpload",
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    ingestionProperties.getDataFormat(),
                    streamSourceInfo.getCompressionType() != null ? streamSourceInfo.getCompressionType() : CompressionType.gz);

            CloudBlockBlob blob = azureStorageClient.uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE),
                    true
            );
            String blobPath = azureStorageClient.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                    blobPath, 0); // TODO: check if we can get the rawDataSize locally - maybe add a countingStream

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

    private long estimateFileRawSize(String filePath, IngestionProperties.DATA_FORMAT format) {
        File file = new File(filePath);
        long fileSize = file.length();
        return (azureStorageClient.getCompression(filePath) != null
                || format == IngestionProperties.DATA_FORMAT.parquet
                || format == IngestionProperties.DATA_FORMAT.orc) ?
                    fileSize * COMPRESSED_FILE_MULTIPLIER : fileSize;
    }

    private String genBlobName(String fileName, String databaseName, String tableName, String dataFormat, CompressionType compressionType) {
        return String.format("%s__%s__%s__%s.%s.%s", databaseName, tableName, UUID.randomUUID().toString(), fileName, dataFormat, compressionType);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validate();
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            new CsvRoutines().write(resultSetSourceInfo.getResultSet(), byteArrayOutputStream);
            byteArrayOutputStream.flush();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(byteArrayInputStream);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String msg = "Failed to read from ResultSet.";
            log.error(msg, ex);
            throw new IngestionClientException(msg, ex);
        }
    }

    @Override
    public void close() {
        this.resourceManager.close();
    }
}
