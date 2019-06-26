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
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

class IngestClientImpl implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
            ingestionBlobInfo.rawDataSize = blobSourceInfo.getRawSizeInBytes() > 0L ? blobSourceInfo.getRawSizeInBytes()
                    : Utils.estimateBlobRawSize(azureStorageClient, blobSourceInfo.getBlobPath());
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
                status.setIngestionSourcePath(SecurityUtils.removeSecretsFromUrl(blobSourceInfo.getBlobPath()));

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

            File file = new File(filePath);
            String blobName = genBlobName(
                    file.getName(), ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageClient.uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE));
            String blobPath = azureStorageClient.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    Utils.estimateFileRawSize(azureStorageClient, filePath);

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
                    "StreamUpload", ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageClient.uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE),
                    true
            );
            String blobPath = azureStorageClient.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                    blobPath, 0); // TODO: check if we can get the rawDataSize locally

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

    private String genBlobName(String fileName, String databaseName, String tableName) {
        return String.format("%s__%s__%s__%s", databaseName, tableName, UUID.randomUUID().toString(), fileName);
    }

    public IngestionResult ingestFromResultSet(
            ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        return ingestFromResultSet(resultSetSourceInfo, ingestionProperties, "");
    }

    public IngestionResult ingestFromResultSet(
            ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties, String tempStoragePath)
            throws IngestionClientException, IngestionServiceException {
        try {
            Objects.requireNonNull(resultSetSourceInfo, "resultSetSourceInfo cannot be null");
            resultSetSourceInfo.validate();

            File tempFile;

            if (StringUtils.isBlank(tempStoragePath)) {
                tempFile = File.createTempFile("kusto-resultset", ".csv.gz");
            } else {
                log.debug("Temp file will be created in a user specified folder: {}", tempStoragePath);
                tempFile = File.createTempFile("kusto-resultset", ".csv.gz", new File(tempStoragePath));
            }

            FileOutputStream fos = new FileOutputStream(tempFile, false);
            GZIPOutputStream gzipos = new GZIPOutputStream(fos);
            Writer writer = new OutputStreamWriter(new BufferedOutputStream(gzipos), StandardCharsets.UTF_8);
            log.debug("Writing resultset to temp csv file: {}", tempFile.getAbsolutePath());

            long numberOfChars = writeResultSetToWriterAsCsv(resultSetSourceInfo.getResultSet(), writer, false);

            // utf8 chars are 2 bytes each
            FileSourceInfo fileSourceInfo = new FileSourceInfo(tempFile.getAbsolutePath(), numberOfChars * 2);
            IngestionResult ingestionResult = ingestFromFile(fileSourceInfo, ingestionProperties);

            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();

            return ingestionResult;
        } catch (IngestionClientException | IngestionServiceException ex) {
            log.error("Unexpected error when ingesting a result set.", ex);
            throw ex;
        } catch (IOException ex) {
            String msg = "Failed to write or delete local file";
            log.error(msg, ex);
            throw new IngestionClientException(msg);
        }
    }

    long writeResultSetToWriterAsCsv(ResultSet resultSet, Writer writer, boolean includeHeaderAsFirstRow) throws IngestionClientException {
        return Utils.writeResultSetToWriterAsCsv(resultSet, writer, includeHeaderAsFirstRow);
    }
}
