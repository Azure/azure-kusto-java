// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.univocity.parsers.csv.CsvRoutines;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class QueuedIngestClient extends IngestClientBase implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;
    private final AzureStorageClient azureStorageClient;
    public static final String EXPECTED_SERVICE_TYPE = "DataManagement";

    QueuedIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        Client client = ClientFactory.createClient(csb);
        this.resourceManager = new ResourceManager(client);
        this.azureStorageClient = new AzureStorageClient();
        this.connectionDataSource = csb.getClusterUrl();
    }

    QueuedIngestClient(ResourceManager resourceManager, AzureStorageClient azureStorageClient) {
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

            String id = ingestionBlobInfo.id.toString();
            IngestionStatus status = new IngestionStatus();
            status.database = ingestionProperties.getDatabaseName();
            status.table = ingestionProperties.getTableName();
            status.status = OperationStatus.Queued;
            status.updatedOn = Date.from(Instant.now());
            status.ingestionSourceId = ingestionBlobInfo.id;
            status.setIngestionSourcePath(urlWithoutSecrets);
            boolean reportToTable = ingestionBlobInfo.reportLevel != IngestionProperties.IngestionReportLevel.None
                && ingestionProperties.getReportMethod() != IngestionProperties.IngestionReportMethod.Queue;
            if (reportToTable) {
                status.status = OperationStatus.Pending;
                String tableStatusUri = resourceManager
                        .getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE);
                ingestionBlobInfo.IngestionStatusInTable = new IngestionStatusInTableDescription();
                ingestionBlobInfo.IngestionStatusInTable.TableConnectionString = tableStatusUri;
                ingestionBlobInfo.IngestionStatusInTable.RowKey = id;
                ingestionBlobInfo.IngestionStatusInTable.PartitionKey = id;
                azureStorageClient.azureTableInsertEntity(tableStatusUri, new TableEntity(id, id).setProperties(status.getEntityProperties()));
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            azureStorageClient.postMessageToQueue(
                    resourceManager
                            .getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);
            return reportToTable
                    ? new TableReportIngestionResult(tableStatuses)
                    : new IngestionStatusResult(status);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from blob", e);
        } catch (IngestionServiceException e) {
            validateEndpointServiceType(connectionDataSource, EXPECTED_SERVICE_TYPE);
            throw e;
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
            CompressionType sourceCompressionType = IngestionUtils.getCompression(filePath);
            boolean shouldCompress = AzureStorageClient.shouldCompress(sourceCompressionType, ingestionProperties.getDataFormat());

            File file = new File(filePath);
            String blobName = genBlobName(
                    file.getName(),
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    ingestionProperties.getDataFormat(),
                    shouldCompress ? CompressionType.gz : sourceCompressionType);
            ContainerWithSas container = resourceManager.getTempStorage();
            String blobPath;
            BlobClient blobClient = azureStorageClient.uploadLocalFileToBlob(file, blobName,
                    container.getContainer(), shouldCompress);
            blobPath = azureStorageClient.getBlobPathWithSas(blobClient.getBlobUrl(),container.getSas());
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(filePath, IngestionProperties.DataFormat.valueOf(ingestionProperties.getDataFormat()));

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);
        } catch (IOException e) {
            throw new IngestionClientException("Failed to ingest from file", e);
        } catch (IngestionServiceException e) {
            validateEndpointServiceType(connectionDataSource, EXPECTED_SERVICE_TYPE);
            throw e;
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
            if (streamSourceInfo.getStream() == null) {
                throw new IngestionClientException("The provided stream is null.");
            } else if (streamSourceInfo.getStream().available() <= 0) {
                throw new IngestionClientException("The provided stream is empty.");
            }
            boolean shouldCompress = AzureStorageClient.shouldCompress(streamSourceInfo.getCompressionType(), ingestionProperties.getDataFormat());

            String blobName = genBlobName(
                    "StreamUpload",
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    ingestionProperties.getDataFormat(),
                    shouldCompress ? CompressionType.gz : streamSourceInfo.getCompressionType());
            ContainerWithSas container = resourceManager.getTempStorage();

            azureStorageClient.uploadStreamToBlob(
                streamSourceInfo.getStream(),
                blobName,
                container.getContainer(),
                shouldCompress
            );
            String blobPath = azureStorageClient.getBlobPathWithSas(container.getSas(), blobName);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                    blobPath, 0); // TODO: check if we can get the rawDataSize locally - maybe add a countingStream

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from stream", e);
        } catch (IngestionServiceException e) {
            validateEndpointServiceType(connectionDataSource, EXPECTED_SERVICE_TYPE);
            throw e;
        }
    }

    private long estimateFileRawSize(String filePath, IngestionProperties.DataFormat format) {
        long fileSize = new File(filePath).length();
        return (IngestionUtils.getCompression(filePath) != null
                || format == IngestionProperties.DataFormat.parquet
                || format == IngestionProperties.DataFormat.orc) ?
                fileSize * COMPRESSED_FILE_MULTIPLIER : fileSize;
    }

    String genBlobName(String fileName, String databaseName, String tableName, String dataFormat, CompressionType compressionType) {
        return String.format("%s__%s__%s__%s%s%s",
                databaseName,
                tableName,
                UUID.randomUUID().toString(),
                fileName,
                dataFormat == null ? "" : "." + dataFormat,
                compressionType == null ? "" : "." + compressionType);
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

            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(byteArrayInputStream, false, resultSetSourceInfo.getSourceId());
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String msg = "Failed to read from ResultSet.";
            log.error(msg, ex);
            throw new IngestionClientException(msg, ex);
        }
    }

    @Override
    protected String emendEndpointUri(URIBuilder existingEndpoint) {
        existingEndpoint.setHost(INGEST_PREFIX + existingEndpoint.getHost());
        return existingEndpoint.toString();
    }

    @Override
    protected String retrieveServiceType() throws IngestionServiceException, IngestionClientException {
        if (resourceManager != null) {
            return resourceManager.retrieveServiceType();
        }
        return null;
    }

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
        this.resourceManager.close();
    }
}