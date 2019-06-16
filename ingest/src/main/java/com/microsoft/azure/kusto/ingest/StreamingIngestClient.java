package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.*;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
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
    private Client client;
    private List<String> mappingRequiredFormats = Arrays.asList("json", "singlejson", "avro");

    public StreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        this.client = ClientFactory.createClient(csb);
        this.azureStorageClient = new AzureStorageClient();
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();

        try{
            String filePath = fileSourceInfo.getFilePath();
            InputStream stream = new FileInputStream(filePath);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false, fileSourceInfo.getSourceId());
            streamSourceInfo.setIsCompressed(this.azureStorageClient.isCompressed(filePath));
            streamSourceInfo.setSize(fileSourceInfo.getRawSizeInBytes());
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        }
        catch (FileNotFoundException e) {
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        try
        {
            String blobPath = blobSourceInfo.getBlobPath();
            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(new URI(blobPath));
            CloudBlobContainer container = cloudBlockBlob.getContainer();
            BlobInputStream blobInputStream = container.getBlockBlobReference(blobPath).openInputStream();

            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(blobInputStream, false, blobSourceInfo.getSourceId());

            streamSourceInfo.setSize(this.azureStorageClient.getBlobSize(blobPath));
            streamSourceInfo.setIsCompressed(this.azureStorageClient.isCompressed(blobPath.split("\\?")[0]));

            return ingestFromStream(streamSourceInfo, ingestionProperties);

        } catch (URISyntaxException e) {
            throw new IngestionClientException("URTSyntaxExpression - check URI.", e);
        } catch (StorageException e) {
            throw new IngestionClientException("StorageException", e);
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

            long numberOfChars = Utils.resultSetToCsv(resultSetSourceInfo.getResultSet(), writer, false);

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

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        String format = getFormat(ingestionProperties);
        String mappingReference = getMappingReference(ingestionProperties, format);

        try{
            InputStream stream = streamSourceInfo.getStream();
            if (stream == null || stream.available() <= 0) {
                throw new IngestionClientException("Stream is null or empty.");
            }

            ClientRequestProperties properties = new ClientRequestProperties();
            properties.setOption("isCompressed", streamSourceInfo.getIsCompressed());
            long size = streamSourceInfo.getSize() < 0 ? stream.available() : streamSourceInfo.getSize();

            properties.setOption("Content-Length", size);

            this.client.executeStreamingIngest(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    stream,
                    format,
                    properties,
                    mappingReference,
                    streamSourceInfo.isLeaveOpen());
        }catch (IOException e)
        {
            throw new IngestionClientException("Failed to ingest from stream, check stream validity.",e);
        }
        catch (DataClientException e) {
            throw new IngestionClientException(e.getMessage(),e);
        } catch (DataServiceException e) {
            throw new IngestionServiceException(e.getMessage(),e);
        }

        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.status = OperationStatus.Succeeded;
        ingestionStatus.table = ingestionProperties.getTableName();
        ingestionStatus.database = ingestionProperties.getDatabaseName();
        return new IngestionStatusResult(ingestionStatus);
    }

    private String getFormat(IngestionProperties ingestionProperties)
    {
        String format = ingestionProperties.getDataFormat();
        if (format == null)
        {
            return "csv";
        }
        return format;
    }

    private String getMappingReference(IngestionProperties ingestionProperties, String format) throws IngestionClientException {
        String mappingReference = ingestionProperties.getIngestionMapping().IngestionMappingReference;
        if (mappingRequiredFormats.contains(format) && StringUtils.isAnyEmpty(mappingReference))
        {
            throw new IngestionClientException("Mapping reference must be specified for json, singlejson and avro formats.");
        }
        return mappingReference;
    }
}
