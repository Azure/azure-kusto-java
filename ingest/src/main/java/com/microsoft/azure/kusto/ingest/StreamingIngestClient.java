package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class StreamingIngestClient implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private AzureStorageClient azureStorageClient;
    private StreamingIngestProvider streamingIngestProvider;
    private List<String> mappingRequiredFormats = Arrays.asList("json", "singlejson", "avro");

    StreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        this.streamingIngestProvider = ClientFactory.createStreamingIngestProvider(csb);
        this.azureStorageClient = new AzureStorageClient();
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            String filePath = fileSourceInfo.getFilePath();
            InputStream stream = new FileInputStream(filePath);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false, fileSourceInfo.getSourceId());
            streamSourceInfo.setIsCompressed(this.azureStorageClient.isCompressed(filePath));
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {

        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            String blobPath = blobSourceInfo.getBlobPath();
            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(new URI(blobPath));
            InputStream stream = cloudBlockBlob.openInputStream();
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false, blobSourceInfo.getSourceId());
            streamSourceInfo.setIsCompressed(this.azureStorageClient.isCompressed(blobPath));
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (URISyntaxException | IllegalArgumentException e) {
            String msg = "Unexpected error when ingesting a blob - Invalid blob path.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        } catch (StorageException e) {
            String msg = "Unexpected Storage error when ingesting a blob.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        }
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return ingestFromResultSet(resultSetSourceInfo, ingestionProperties, "");
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties, String tempStoragePath) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validate();

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipos = new GZIPOutputStream(byteArrayOutputStream);
            Writer writer = new OutputStreamWriter(new BufferedOutputStream(gzipos), StandardCharsets.UTF_8);
            Utils.writeResultSetToWriterAsCsv(resultSetSourceInfo.getResultSet(), writer, false);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(byteArrayInputStream);
            streamSourceInfo.setIsCompressed(true);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String msg = "Failed to write or delete local file";
            log.error(msg, ex);
            throw new IngestionClientException(msg, ex);
        }
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        String format = getFormat(ingestionProperties);
        String mappingReference = getMappingReference(ingestionProperties, format);
        try {
            log.debug("Executing streaming ingest");
            this.streamingIngestProvider.executeStreamingIngest(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    streamSourceInfo.getStream(),
                    null,
                    format,
                    !streamSourceInfo.getIsCompressed(),
                    mappingReference,
                    streamSourceInfo.isLeaveOpen());
        } catch (DataClientException e) {
            log.error(e.getMessage(), e);
            throw new IngestionClientException(e.getMessage(), e);
        } catch (DataServiceException e) {
            log.error(e.getMessage(), e);
            throw new IngestionServiceException(e.getMessage(), e);
        }

        log.debug("Stream was ingested successfully.");
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.status = OperationStatus.Succeeded;
        ingestionStatus.table = ingestionProperties.getTableName();
        ingestionStatus.database = ingestionProperties.getDatabaseName();
        return new IngestionStatusResult(ingestionStatus);
    }

    private String getFormat(IngestionProperties ingestionProperties) {
        String format = ingestionProperties.getDataFormat();
        if (format == null) {
            return "csv";
        }
        return format;
    }

    private String getMappingReference(IngestionProperties ingestionProperties, String format) throws IngestionClientException {
        String mappingReference = ingestionProperties.getIngestionMapping().IngestionMappingReference;
        if (mappingRequiredFormats.contains(format) && StringUtils.isBlank(mappingReference)) {
            log.error("Mapping reference must be specified for json, singlejson and avro formats.");
            throw new IngestionClientException("Mapping reference must be specified for json, singlejson and avro formats.");
        }
        return mappingReference;
    }
}
