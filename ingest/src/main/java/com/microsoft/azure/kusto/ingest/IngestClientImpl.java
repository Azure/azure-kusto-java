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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

class IngestClientImpl implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int COMPRESSED_FILE_MULTIPLIER = 11;
    private final ResourceManager resourceManager;
    private AzureStorageHelper azureStorageHelper;

    IngestClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        log.info("Creating a new IngestClient");
        Client client = ClientFactory.createClient(csb);
        this.resourceManager = new ResourceManager(client);
        this.azureStorageHelper = new AzureStorageHelper();
    }

    IngestClientImpl(ResourceManager resourceManager) {
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        azureStorageHelper = new AzureStorageHelper();
    }

    IngestClientImpl(ResourceManager resourceManager, AzureStorageHelper azureStorageHelper) {
        log.info("Creating a new IngestClient");
        this.resourceManager = resourceManager;
        this.azureStorageHelper = azureStorageHelper;
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {

        // Argument validation:
        if (blobSourceInfo == null || ingestionProperties == null) {
            throw new IllegalArgumentException("blobSourceInfo or ingestionProperties is null");
        }
        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            ingestionProperties.setAuthorizationContextToken(resourceManager.getIdentityToken());
            List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

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
                String tableStatusUri = resourceManager.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE);
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

                azureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

            azureStorageHelper.postMessageToQueue(
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE)
                    , serializedIngestionBlobInfo);
            return new TableReportIngestionResult(tableStatuses);

        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromBlob()", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromBlob()", e);
        }
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        if (fileSourceInfo == null || ingestionProperties == null) {
            throw new IllegalArgumentException("fileSourceInfo or ingestionProperties is null");
        }
        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            String fileName = (new File(fileSourceInfo.getFilePath())).getName();
            String blobName = genBlobName(fileName, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageHelper.uploadLocalFileToBlob(fileSourceInfo.getFilePath(), blobName, resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE));
            String blobPath = azureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = fileSourceInfo.getRawSizeInBytes() > 0L ? fileSourceInfo.getRawSizeInBytes() :
                    estimateFileRawSize(fileSourceInfo.getFilePath());

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, rawDataSize, fileSourceInfo.getSourceId());

            return ingestFromBlob(blobSourceInfo, ingestionProperties);

        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromFile()", e);
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromFile()", e);
        }
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        // Argument validation:
        if (streamSourceInfo == null || ingestionProperties == null) {
            throw new IllegalArgumentException("streamSourceInfo or ingestionProperties is null");
        }
        streamSourceInfo.validate();
        ingestionProperties.validate();

        try {
            IngestionResult ingestionResult;
            if (streamSourceInfo.getStream() == null || streamSourceInfo.getStream().available() <= 0) {
                throw new IngestionClientException("Stream is empty");
            }
            String blobName = genBlobName("StreamUpload", ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = azureStorageHelper.uploadStreamToBlob(
                    streamSourceInfo.getStream(),
                    blobName,
                    resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE),
                    true
            );
            String blobPath = azureStorageHelper.getBlobPathWithSas(blob);
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, 0); // TODO: check if we can get the rawDataSize locally

            ingestionResult = ingestFromBlob(blobSourceInfo, ingestionProperties);
            if (!streamSourceInfo.isLeaveOpen()) {
                streamSourceInfo.getStream().close();
            }
            return ingestionResult;
        } catch (IOException | URISyntaxException e) {
            throw new IngestionClientException("Error in ingestFromStream()", e);
        } catch (StorageException e) {
            throw new IngestionServiceException("Error in ingestFromStream()", e);
        }
    }

    private Long estimateBlobRawSize(@org.jetbrains.annotations.NotNull BlobSourceInfo blobSourceInfo) throws IngestionClientException, IngestionServiceException {
        try {
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
        } catch (StorageException e) {
            throw new IngestionServiceException("Error in estimateBlobRawSize", e);
        } catch (URISyntaxException e) {
            throw new IngestionClientException("Error in estimateBlobRawSize", e);
        }
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

    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return ingestFromResultSet(resultSetSourceInfo, ingestionProperties, "");
    }

    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties, String tempStoragePath) throws IngestionClientException, IngestionServiceException {
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

            long numberOfChars = resultSetToCsv(resultSetSourceInfo.getResultSet(), writer, false);

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

    long resultSetToCsv(ResultSet resultSet, Writer writer, boolean includeHeaderAsFirstRow) throws IngestionClientException {
        final String LINE_SEPARATOR = System.getProperty("line.separator");

        try {
            String columnSeparator = "";

            ResultSetMetaData metaData = resultSet.getMetaData();
            int numberOfColumns = metaData.getColumnCount();

            if (includeHeaderAsFirstRow) {
                for (int column = 0; column < numberOfColumns; column++) {
                    writer.write(columnSeparator);
                    writer.write(metaData.getColumnLabel(column + 1));

                    columnSeparator = ",";
                }

                writer.write(LINE_SEPARATOR);
            }

            int numberOfRecords = 0;
            long numberOfChars = 0;

            // Get all rows.
            while (resultSet.next()) {
                numberOfChars += writeResultSetRow(resultSet, writer, numberOfColumns);
                writer.write(LINE_SEPARATOR);
                // Increment row count
                numberOfRecords++;
            }

            log.debug("Number of chars written from column values: {}", numberOfChars);

            long totalNumberOfChars = numberOfChars + numberOfRecords * LINE_SEPARATOR.length();

            log.debug("Wrote resultset to file. CharsCount: {}, ColumnCount: {}, RecordCount: {}"
                    , numberOfChars, numberOfColumns, numberOfRecords);

            return totalNumberOfChars;
        } catch (Exception ex) {
            String msg = "Unexpected error when writing result set to temporary file.";
            log.error(msg, ex);
            throw new IngestionClientException(msg);
        } finally {
            try {
                writer.close();
            } catch (IOException e) { /* ignore */
            }
        }
    }

    private int writeResultSetRow(ResultSet resultSet, Writer writer, int numberOfColumns) throws IOException, SQLException {
        int numberOfChars = 0;
        String columnString;
        String columnSeparator = "";

        for (int i = 1; i <= numberOfColumns; i++) {
            writer.write(columnSeparator);
            writer.write('"');
            columnString = resultSet.getObject(i).toString().replace("\"", "\"\"");
            writer.write(columnString);
            writer.write('"');

            columnSeparator = ",";
            numberOfChars += columnString.length();
        }

        return numberOfChars
                + numberOfColumns * 2 * columnSeparator.length() // 2 " per column
                + numberOfColumns - 1 // last column doesn't have a separator
                ;
    }

}
