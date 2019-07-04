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
import java.sql.ResultSet;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class StreamingIngestClient implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private AzureStorageClient azureStorageClient;
    private StreamingIngestProvider streamingIngestProvider;
    private final List<String> mappingRequiredFormats = Arrays.asList("json", "singlejson", "avro");
    private static final int STREAM_COMPRESS_BUFFER_SIZE = 16 * 1024;

    StreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        this.streamingIngestProvider = ClientFactory.createStreamingIngestProvider(csb);
        this.azureStorageClient = new AzureStorageClient();
    }

    StreamingIngestClient(StreamingIngestProvider streamingIngestProvider) {
        log.info("Creating a new StreamingIngestClient");
        this.streamingIngestProvider = streamingIngestProvider;
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
            File file = new File(filePath);
            if (file.length() == 0) {
                String message = "Empty file.";
                log.error(message);
                throw new IngestionClientException(message);
            }
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
            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(new URI(blobSourceInfo.getBlobPath()));
            return ingestFromBlob(blobSourceInfo, ingestionProperties, cloudBlockBlob);
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
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            Writer writer = new OutputStreamWriter(new BufferedOutputStream(gzipOutputStream), StandardCharsets.UTF_8);
            if (writeResultSetToWriterAsCsv(resultSetSourceInfo.getResultSet(), writer, false) == 0) {
                String message = "Empty ResultSet.";
                log.error(message);
                throw new IngestionClientException(message);
            }
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(byteArrayInputStream, false, resultSetSourceInfo.getSourceId());
            streamSourceInfo.setIsCompressed(true);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String message = "Unexpected error when ingesting a ResultSet.";
            log.error(message, ex);
            throw new IngestionClientException(message, ex);
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
            InputStream stream = (streamSourceInfo.getIsCompressed()) ? streamSourceInfo.getStream() : compressStream(streamSourceInfo.getStream(), streamSourceInfo.isLeaveOpen());
            log.debug("Executing streaming ingest.");
            this.streamingIngestProvider.executeStreamingIngest(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    stream,
                    null,
                    format,
                    mappingReference,
                    streamSourceInfo.getIsCompressed() && streamSourceInfo.isLeaveOpen());
        } catch (DataClientException | IOException e) {
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
        IngestionMapping ingestionMapping = ingestionProperties.getIngestionMapping();
        String mappingReference = ingestionMapping.getIngestionMappingReference();
        IngestionMapping.IngestionMappingKind ingestionMappingKind = ingestionMapping.getIngestionMappingKind();
        if (mappingRequiredFormats.contains(format)) {
            String message = null;
            if (!format.equals(ingestionMappingKind.name())) {
                message = String.format("Wrong ingestion mapping for format %s, found %s mapping kind.", format, ingestionMappingKind.name());
            }
            if (StringUtils.isBlank(mappingReference)) {
                message = String.format("Mapping reference must be specified for %s format.", format);
            }
            if (message != null) {
                log.error(message);
                throw new IngestionClientException(message);
            }
        }
        return mappingReference;
    }

    private InputStream compressStream(InputStream uncompressedStream, boolean leaveOpen) throws IngestionClientException, IOException {
        log.debug("Compressing the stream.");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        byte[] b = new byte[STREAM_COMPRESS_BUFFER_SIZE];
        int read = uncompressedStream.read(b);
        if (read == -1) {
            String message = "Empty stream.";
            log.error(message);
            throw new IngestionClientException(message);
        }
        do {
            gzipOutputStream.write(b, 0, read);
        } while ((read = uncompressedStream.read(b)) != -1);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.close();
        if (!leaveOpen) {
            uncompressedStream.close();
        }
        return inputStream;
    }

    long writeResultSetToWriterAsCsv(ResultSet resultSet, Writer writer, boolean includeHeaderAsFirstRow) throws IngestionClientException {
        return ResultSetUtils.writeResultSetToWriterAsCsv(resultSet, writer, includeHeaderAsFirstRow);
    }

    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties, CloudBlockBlob cloudBlockBlob) throws IngestionClientException, IngestionServiceException, StorageException {
        String blobPath = blobSourceInfo.getBlobPath();
        cloudBlockBlob.downloadAttributes();
        if (cloudBlockBlob.getProperties().getLength() == 0) {
            String message = "Empty blob.";
            log.error(message);
            throw new IngestionClientException(message);
        }
        InputStream stream = cloudBlockBlob.openInputStream();
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false, blobSourceInfo.getSourceId());
        streamSourceInfo.setIsCompressed(this.azureStorageClient.isCompressed(blobPath));
        return ingestFromStream(streamSourceInfo, ingestionProperties);
    }
}
