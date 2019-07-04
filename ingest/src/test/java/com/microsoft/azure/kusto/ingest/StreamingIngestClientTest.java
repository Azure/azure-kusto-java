package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.StreamingIngestProvider;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class StreamingIngestClientTest {

    private static StreamingIngestClient streamingIngestClient;
    private IngestionProperties ingestionProperties;

    @BeforeAll
    static void setUp() throws Exception {
        StreamingIngestProvider streamingIngestProvider = mock(StreamingIngestProvider.class);
        streamingIngestClient = new StreamingIngestClient(streamingIngestProvider);

        when(streamingIngestProvider.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                any(ClientRequestProperties.class), any(String.class), any(String.class), any(boolean.class))).thenReturn(null);
    }

    @BeforeEach
    void setUpEach() {
        ingestionProperties = new IngestionProperties("dbName", "tableName");
    }

    @Test
    void IngestFromStream() throws Exception {
        // Create CSV stream from String and ingest
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Create Compressed CSV stream and ingest
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        byte[] inputArray = Charset.forName("UTF-8").encode("Name, Age, Weight, Height").array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        streamSourceInfo.setStream(inputStream);
        streamSourceInfo.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Create JSON stream from String and ingest
        data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Create Compressed JSON stream and ingest
        byteArrayOutputStream = new ByteArrayOutputStream();
        gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        inputArray = Charset.forName("UTF-8").encode("{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}").array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        streamSourceInfo.setStream(inputStream);
        streamSourceInfo.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    @Test
    void IngestFromStream_StreamSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionProperties_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_JsonFormat_NoMappingReference() {
        // Json format without mapping reference
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for json format."));

        // Json format with incompatible mapping reference
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found csv mapping kind."));
    }

    @Test
    void IngestFromStream_AvroFormat_NoMappingReference() {
        // Avro format without mapping reference
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for avro format."));

        // Avro format with incompatible mapping reference
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format avro, found csv mapping kind."));
    }

    @Test
    void IngestFromStream_EmptyStream() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty stream."));
    }

    @Test
    void IngestFromFile() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        //Ingest CSV file
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest JSON file
        path = resourcesDirectory + "testdata.json";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest Compressed JSON file
        path = resourcesDirectory + "testdata.json";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    @Test
    void IngestFromFile_FileSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        // Null Path
        FileSourceInfo fileSourceInfo1 = new FileSourceInfo(null, 0);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        // Blank path
        FileSourceInfo fileSourceInfo2 = new FileSourceInfo("", 0);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_IngestionProperties_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        //Ingest CSV file
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_JsonFormat_NoMappingReference() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        // Json format without mapping reference
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for json format."));

        // Json format with incompatible mapping reference
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found csv mapping kind."));
    }

    @Test
    void IngestFromFile_EmptyFile() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "empty.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty file."));
    }

    @Test
    void IngestFromBlob() throws Exception {
        CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        String blobPath = "https://storageaccount.blob.core.windows.net/container/blob.csv";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath);

        BlobProperties blobProperties = mock(BlobProperties.class);
        when(blobProperties.getLength()).thenReturn((long) 1000);

        BlobInputStream blobInputStream = mock(BlobInputStream.class);
        when(blobInputStream.read(any(byte[].class))).thenReturn(10).thenReturn(-1);

        doNothing().when(cloudBlockBlob).downloadAttributes();
        when(cloudBlockBlob.getProperties()).thenReturn(blobProperties);
        when(cloudBlockBlob.openInputStream()).thenReturn(blobInputStream);

        OperationStatus status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties, cloudBlockBlob).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    @Test
    void IngestFromBlob_BlobSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        // Null Path
        BlobSourceInfo blobSourceInfo1 = new BlobSourceInfo(null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        // Blank path
        BlobSourceInfo blobSourceInfo2 = new BlobSourceInfo("");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_IngestionProperties_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_InvalidBlobPath_IngestionClientException() {
        String path = "wrongURI";
        BlobSourceInfo blobSourceInfo1 = new BlobSourceInfo(path);

        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo1, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");

        assertTrue(ingestionClientException.getMessage().contains("Unexpected error when ingesting a blob - Invalid blob path."));

        path = "https://storageaccount.blob.core.windows.net/container/blob.csv";
        BlobSourceInfo blobSourceInfo2 = new BlobSourceInfo(path);

        ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo2, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Unexpected Storage error when ingesting a blob."));
    }

    @Test
    void IngestFromBlob_EmptyBlob() throws Exception {
        CloudBlockBlob cloudBlockBlob = mock(CloudBlockBlob.class);
        String blobPath = "https://storageaccount.blob.core.windows.net/container/blob.csv";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath);

        BlobProperties blobProperties = mock(BlobProperties.class);
        when(blobProperties.getLength()).thenReturn((long) 0);

        doNothing().when(cloudBlockBlob).downloadAttributes();
        when(cloudBlockBlob.getProperties()).thenReturn(blobProperties);

        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties, cloudBlockBlob),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty blob."));
    }

    @Test
    void IngestFromResultSet() throws Exception {
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(1)).thenReturn("Name");
        when(resultSet.getObject(2)).thenReturn("Age");
        when(resultSet.getObject(3)).thenReturn("Weight");

        when(resultSetMetaData.getColumnCount()).thenReturn(3);

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        OperationStatus status = streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    @Test
    void IngestFromResultSet_ResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_IngestionProperties_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");

        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_EmptyResultSet() throws Exception {
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(0);

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty ResultSet."));
    }
}
