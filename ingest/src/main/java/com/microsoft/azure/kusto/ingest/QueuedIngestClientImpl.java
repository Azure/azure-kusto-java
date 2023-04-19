// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusInTableDescription;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.result.TableReportIngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import com.microsoft.azure.kusto.ingest.utils.SecurityUtils;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import com.univocity.parsers.csv.CsvRoutines;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class QueuedIngestClientImpl extends IngestClientBase implements QueuedIngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;
    private final AzureStorageClient azureStorageClient;

    QueuedIngestClientImpl(ConnectionStringBuilder csb, @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        this(csb, properties == null ? null : HttpClientFactory.create(properties),autoCorrectEndpoint);
    }

    QueuedIngestClientImpl(ConnectionStringBuilder csb, CloseableHttpClient httpClient, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        ConnectionStringBuilder csbWithEndpoint = new ConnectionStringBuilder(csb);
        csbWithEndpoint.setClusterUrl(autoCorrectEndpoint ?
                getIngestionEndpoint(csbWithEndpoint.getClusterUrl()) : csbWithEndpoint.getClusterUrl());
        Client client = ClientFactory.createClient(csbWithEndpoint, httpClient);
        this.resourceManager = new ResourceManager(client, httpClient);
        this.azureStorageClient = new AzureStorageClient();
        this.connectionDataSource = csbWithEndpoint.getClusterUrl();
    }

    QueuedIngestClientImpl(ResourceManager resourceManager, AzureStorageClient azureStorageClient) {
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        this.azureStorageClient = azureStorageClient;
    }

    public void setQueueRequestOptions(RequestRetryOptions queueRequestOptions) {
        this.resourceManager.setQueueRequestOptions(queueRequestOptions);
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
                ingestionBlobInfo.setRawDataSize(blobSourceInfo.getRawSizeInBytes());
            } else {
                log.warn("Blob '{}' was sent for ingestion without specifying its raw data size", urlWithoutSecrets);
            }

            ingestionBlobInfo.setReportLevel(ingestionProperties.getReportLevel().getKustoValue());
            ingestionBlobInfo.setReportMethod(ingestionProperties.getReportMethod().getKustoValue());
            ingestionBlobInfo.setFlushImmediately(ingestionProperties.getFlushImmediately());
            ingestionBlobInfo.setValidationPolicy(ingestionProperties.getValidationPolicy());
            ingestionBlobInfo.setAdditionalProperties(ingestionProperties.getIngestionProperties());
            if (blobSourceInfo.getSourceId() != null) {
                ingestionBlobInfo.setId(blobSourceInfo.getSourceId());
            }

            String id = ingestionBlobInfo.getId().toString();
            IngestionStatus status = new IngestionStatus();
            status.setDatabase(ingestionProperties.getDatabaseName());
            status.setTable(ingestionProperties.getTableName());
            status.setStatus(OperationStatus.Queued);
            status.setUpdatedOn(Instant.now());
            status.setIngestionSourceId(ingestionBlobInfo.getId());
            status.setIngestionSourcePath(urlWithoutSecrets);
            boolean reportToTable = ingestionProperties.getReportLevel() != IngestionProperties.IngestionReportLevel.NONE &&
                    ingestionProperties.getReportMethod() != IngestionProperties.IngestionReportMethod.QUEUE;
            if (reportToTable) {
                status.setStatus(OperationStatus.Pending);
                TableWithSas statusTable = resourceManager
                        .getStatusTable();
                IngestionStatusInTableDescription ingestionStatusInTable = new IngestionStatusInTableDescription();
                ingestionStatusInTable.setTableClient(statusTable.getTable());
                ingestionStatusInTable.setTableConnectionString(statusTable.getUri());
                ingestionStatusInTable.setPartitionKey(ingestionBlobInfo.getId().toString());
                ingestionStatusInTable.setRowKey(ingestionBlobInfo.getId().toString());
                ingestionBlobInfo.setIngestionStatusInTable(ingestionStatusInTable);
                azureStorageClient.azureTableInsertEntity(statusTable.getTable(), new TableEntity(id, id).setProperties(status.getEntityProperties()));
                tableStatuses.add(ingestionBlobInfo.getIngestionStatusInTable());
            }

            ObjectMapper objectMapper = Utils.getObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            azureStorageClient.postMessageToQueue(
                    resourceManager.getQueue().getQueue(), serializedIngestionBlobInfo);
            return reportToTable
                    ? new TableReportIngestionResult(tableStatuses)
                    : new IngestionStatusResult(status);
        } catch (BlobStorageException e) {
            throw new IngestionServiceException("Failed to ingest from blob", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from blob", e);
        } catch (IngestionServiceException e) {
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
            Ensure.fileExists(filePath);
            CompressionType sourceCompressionType = IngestionUtils.getCompression(filePath);
            IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();
            boolean shouldCompress = shouldCompress(sourceCompressionType, dataFormat);

            File file = new File(filePath);
            String blobName = genBlobName(
                    file.getName(),
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    dataFormat.getKustoValue(), // Used to use an empty string if the DataFormat was empty. Now it can't be empty, with a default of CSV.
                    shouldCompress ? CompressionType.gz : sourceCompressionType);
            ContainerWithSas container = resourceManager.getTempStorage();
            azureStorageClient.uploadLocalFileToBlob(file, blobName,
                    container.getContainer(), shouldCompress);
            String blobPath = container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas();
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes()
                    : estimateFileRawSize(filePath, ingestionProperties.getDataFormat().isCompressible());

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);
        } catch (BlobStorageException e) {
            throw new IngestionServiceException("Failed to ingest from file", e);
        } catch (IOException e) {
            throw new IngestionClientException("Failed to ingest from file", e);
        } catch (IngestionServiceException e) {
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
            IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();
            boolean shouldCompress = shouldCompress(streamSourceInfo.getCompressionType(), dataFormat);

            String blobName = genBlobName(
                    "StreamUpload",
                    ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    dataFormat.getKustoValue(), // Used to use an empty string if the DataFormat was empty. Now it can't be empty, with a default of CSV.
                    shouldCompress ? CompressionType.gz : streamSourceInfo.getCompressionType());
            ContainerWithSas container = resourceManager.getTempStorage();

            azureStorageClient.uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    container.getContainer(),
                    shouldCompress);
            String blobPath = container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas();
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                    blobPath, 0, streamSourceInfo.getSourceId()); // TODO: check if we can get the rawDataSize locally - maybe add a countingStream

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;
        } catch (BlobStorageException e) {
            throw new IngestionServiceException("Failed to ingest from stream", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Failed to ingest from stream", e);
        } catch (IngestionServiceException e) {
            throw e;
        }
    }

    private long estimateFileRawSize(String filePath, boolean isCompressible) {
        long fileSize = new File(filePath).length();
        return (IngestionUtils.getCompression(filePath) != null || !isCompressible) ? fileSize * COMPRESSED_FILE_MULTIPLIER : fileSize;
    }

    String genBlobName(String fileName, String databaseName, String tableName, String dataFormat, CompressionType compressionType) {
        return String.format("%s__%s__%s__%s%s%s",
                databaseName,
                tableName,
                UriUtils.removeExtension(fileName),
                UUID.randomUUID(),
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
        ingestionProperties.validateResultSetProperties();
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

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
        this.resourceManager.close();
    }
}
