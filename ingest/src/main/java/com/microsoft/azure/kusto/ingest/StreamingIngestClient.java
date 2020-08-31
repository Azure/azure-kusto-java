// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.univocity.parsers.csv.CsvRoutines;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.zip.GZIPOutputStream;

public class StreamingIngestClient implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private StreamingClient streamingClient;
    private static final int STREAM_COMPRESS_BUFFER_SIZE = 16 * 1024;
    public static final String EXPECTED_SERVICE_TYPE = "Engine";
    private String endpointServiceType;
    private String suggestedEndpointUri;
    private String connectionDataSource;

    StreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        this.streamingClient = ClientFactory.createStreamingClient(csb);
        this.connectionDataSource = csb.getClusterUrl();
    }

    StreamingIngestClient(StreamingClient streamingClient) {
        log.info("Creating a new StreamingIngestClient");
        this.streamingClient = streamingClient;
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
            streamSourceInfo.setCompressionType(AzureStorageClient.getCompression(filePath));
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        log.warn("Ingesting from blob using the StreamingIngestClient is not recommended, consider using the IngestClient instead.");
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
        // Argument validation:
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validate();
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            new CsvRoutines().write(resultSetSourceInfo.getResultSet(), byteArrayOutputStream);
            byteArrayOutputStream.flush();
            if (byteArrayOutputStream.size() <= 0) {
                String message = "Empty ResultSet.";
                log.error(message);
                throw new IngestionClientException(message);
            }
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
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        String format = getFormat(ingestionProperties);
        String mappingReference = getMappingReference(ingestionProperties, format);
        try {
            InputStream stream = (streamSourceInfo.getCompressionType() != null) ? streamSourceInfo.getStream() : compressStream(streamSourceInfo.getStream(), streamSourceInfo.isLeaveOpen());
            log.debug("Executing streaming ingest.");
            this.streamingClient.executeStreamingIngest(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    stream,
                    null,
                    format,
                    mappingReference,
                    !(streamSourceInfo.getCompressionType() == null || !streamSourceInfo.isLeaveOpen()));

        } catch (DataClientException | IOException e) {
            log.error(e.getMessage(), e);
            throw new IngestionClientException(e.getMessage(), e);
        } catch (DataServiceException e) {
            log.error(e.getMessage(), e);
            if (e.getCause() instanceof DataWebException && "Error in post request".equals(e.getMessage())) {
                validateEndpointServiceType();
            }
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
        if (IngestionMapping.mappingRequiredFormats.contains(format)) {
            String message = null;
            if (!format.equalsIgnoreCase(ingestionMappingKind.name())) {
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
        streamSourceInfo.setCompressionType(AzureStorageClient.getCompression(blobPath));
        return ingestFromStream(streamSourceInfo, ingestionProperties);
    }

    /* TODO yischoen 2020-08-31: This and the following methods as well a supporting class properties, are almost
	 *	identical to QueuedIngestClient.validateEndpointServiceType(). This is because there’s no common Impl parent
	 *	shared by QueuedIngestClient and StreamingIngestClient. Likewise, the ClientImpl implements both the Client
	 *	and StreamingClient Interfaces, but there’s not shared parent Impl. Also, the QueuedIngestClient executes
	 *	commands using the ResourceManager, and the StreamingIngestClient executes commands directly using the client.
	 *	To dedupe, we should add a common IngestClientImpl that both inherit from. We are not merging this PR until we
	 *	decide if such a re-arch is appropriate given how often this feature would be helpful.
	*/
    protected void validateEndpointServiceType() throws IngestionServiceException, IngestionClientException {
        if (StringUtils.isBlank(endpointServiceType)) {
            endpointServiceType = retrieveServiceType();
        }
        if (!EXPECTED_SERVICE_TYPE.equals(endpointServiceType)) {
            String message = String.format(QueuedIngestClient.WRONG_ENDPOINT_MESSAGE, EXPECTED_SERVICE_TYPE, endpointServiceType);
            suggestedEndpointUri = generateEndpointSuggestion(suggestedEndpointUri, connectionDataSource);
            if (StringUtils.isNotBlank(suggestedEndpointUri)) {
                message = String.format("%s: '%s'", message, suggestedEndpointUri);
            } else {
                message += ".";
            }
            throw new IngestionClientException(message);
        }
    }

    protected static String generateEndpointSuggestion(String existingSuggestedEndpointUri, String dataSource) {
        if (existingSuggestedEndpointUri != null) {
            return existingSuggestedEndpointUri;
        }
        // The default is not passing a suggestion to the exception
        String endpointUriToSuggestStr = "";
        if (StringUtils.isNotBlank(dataSource)) {
            URIBuilder endpointUriToSuggest;
            try {
                endpointUriToSuggest = new URIBuilder(dataSource);
                if (endpointUriToSuggest.getHost().startsWith(QueuedIngestClient.INGEST_PREFIX)) {
                    endpointUriToSuggest.setHost(endpointUriToSuggest.getHost().substring(QueuedIngestClient.INGEST_PREFIX.length()));
                }
                endpointUriToSuggestStr = endpointUriToSuggest.toString();
            } catch (URISyntaxException e) {
                log.error("Couldn't parse dataSource '{}', so no suggestion can be made.", dataSource, e);
            }
        }
        return endpointUriToSuggestStr;
    }

    private String retrieveServiceType() throws IngestionServiceException, IngestionClientException {
        if (streamingClient != null) {
            log.info("Getting version to determine endpoint's ServiceType");
            try {
                KustoOperationResult versionResult = streamingClient.execute(Commands.VERSION_SHOW_COMMAND);
                if (versionResult != null && versionResult.hasNext() && !versionResult.getResultTables().isEmpty()) {
                    KustoResultSetTable resultTable = versionResult.next();
                    resultTable.next();
                    return resultTable.getString(ResourceManager.SERVICE_TYPE_COLUMN_NAME);
                }
            } catch (DataServiceException e) {
                throw new IngestionServiceException(e.getIngestionSource(), "Couldn't retrieve ServiceType because of a service exception executing '.show version'", e);
            } catch (DataClientException e) {
                throw new IngestionClientException(e.getIngestionSource(), "Couldn't retrieve ServiceType because of a client exception executing '.show version'", e);
            }
            throw new IngestionServiceException("Couldn't retrieve ServiceType because '.show version' didn't return any records");
        }
        return null;
    }

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
    }
}
