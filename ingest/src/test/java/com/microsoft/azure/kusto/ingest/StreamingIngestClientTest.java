package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StreamingIngestClientTest {

    private static StreamingIngestClient streamingIngestClient;
    private IngestionProperties ingestionProperties;

    @Mock
    private static StreamingClient streamingClientMock;

    @Captor
    private static ArgumentCaptor<InputStream> argumentCaptor;

    @BeforeAll
    static void setUp() {
        streamingClientMock = mock(StreamingClient.class);
        streamingIngestClient = new StreamingIngestClient(streamingClientMock);
        argumentCaptor = ArgumentCaptor.forClass((InputStream.class));
    }

    @BeforeEach
    void setUpEach() throws Exception {
        ingestionProperties = new IngestionProperties("dbName", "tableName");

        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class))).thenReturn(null);

        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenReturn(null);
    }

    @Test
    void IngestFromStream_CsvStream() throws Exception {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        /* In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed.
         * When the given stream content is already compressed, the user must specify that in the stream source info.
         * This method verifies if the stream was compressed correctly.
         */
        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromStream_CompressedCsvStream() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        String data = "Name, Age, Weight, Height";
        byte[] inputArray = Charset.forName("UTF-8").encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        // When ingesting compressed data, we should set this property true to avoid double compression.
        streamSourceInfo.setCompressionType(CompressionType.gz);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromStream_JsonStream() throws Exception {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromStream_CompressedJsonStream() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        byte[] inputArray = Charset.forName("UTF-8").encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        streamSourceInfo.setCompressionType(CompressionType.gz);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromStream_NullStreamSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_NullIngestionProperties_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithNullDatabase_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithEmptyDatabase_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithNullTable_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithEmptyTable_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_JsonNoMappingReference_IngestionClientException() {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for json format."));
    }

    @Test
    void IngestFromStream_JsonWrongMappingKind_IngestionClientException() {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found csv mapping kind."));
    }

    @Test
    void IngestFromStream_AvroNoMappingReference_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for avro format."));
    }

    @Test
    void IngestFromStream_AvroWrongMappingKind_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.avro);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format avro, found csv mapping kind."));
    }

    @Test
    void IngestFromStream_EmptyStream_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Empty stream."));
    }

    @Test
    void IngestFromStream_CaughtDataClientException_IngestionClientException() throws Exception {
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenThrow(DataClientException.class);

        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_CaughtDataServiceException_IngestionServiceException() throws Exception {
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenThrow(DataServiceException.class);

        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IngestionServiceException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_Csv() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class));
    }

    @Test
    void IngestFromFile_Json() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class));
    }

    @Test
    void IngestFromFile_CompressedJson() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.json);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class));
    }

    @Test
    void IngestFromFile_NullFileSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_FileSourceInfoWithNullFilePath_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo1 = new FileSourceInfo(null, 0);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_FileSourceInfoWithBlankFilePath_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo2 = new FileSourceInfo("", 0);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_NullIngestionProperties_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_IngestionPropertiesWithNullDatabase_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_IngestionPropertiesWithBlankDatabase_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_IngestionPropertiesWithNullTable_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_IngestionPropertiesWithBlankTable_IllegalArgumentException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromFile_JsonNoMappingReference_IngestionClientException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for json format."));
    }

    @Test
    void IngestFromFile_JsonWrongMappingKind_IngestionClientException() {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DATA_FORMAT.json);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found csv mapping kind."));
    }

    @Test
    void IngestFromFile_EmptyFile_IngestionClientException() {
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
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class));
    }

    @Test
    void IngestFromBlob_NullBlobSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_BlobSourceInfoWithNullBlobPath_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo1 = new BlobSourceInfo(null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo1, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_BlobSourceInfoWithBlankBlobPath_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo2 = new BlobSourceInfo("");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo2, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_NullIngestionProperties_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_IngestionPropertiesWithNullDatabase_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_IngestionPropertiesWithBlankDatabase_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_IngestionPropertiesWithNullTable_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromBlob_IngestionPropertiesWithEmptyTable_IllegalArgumentException() {
        String path = "blobPath";
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(path);
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
    }

    @Test
    void IngestFromBlob_BlobNotFound_IngestionClientException() {
        String path = "https://storageaccount.blob.core.windows.net/container/blob.csv";
        BlobSourceInfo blobSourceInfo2 = new BlobSourceInfo(path);

        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromBlob(blobSourceInfo2, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Unexpected Storage error when ingesting a blob."));
    }

    @Test
    void IngestFromBlob_EmptyBlob_IngestClientException() throws Exception {
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

        ArgumentCaptor<InputStream> argumentCaptor = ArgumentCaptor.forClass(InputStream.class);

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        OperationStatus status = streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(status, OperationStatus.Succeeded);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, "Name,Age,Weight");
    }

    @Test
    void IngestFromResultSet_NullResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(null, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_NullIngestionProperties_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_IngestionPropertiesWithNullDatabase_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_IngestionPropertiesWithEmptyDatabase_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_IngestionPropertiesWithNullTable_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_IngestionPropertiesWithEmptyTable_IllegalArgumentException() {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromResultSet_EmptyResultSet_IngestionClientException() throws Exception {
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

    // Verifies the given stream is compressed correctly and matches the anticipated data content
    private void verifyCompressedStreamContent(InputStream compressedStream, String data) throws Exception {
        GZIPInputStream gzipInputStream = new GZIPInputStream(compressedStream);
        byte[] buffer = new byte[1];
        byte[] bytes = new byte[100];
        int index = 0;
        while ((gzipInputStream.read(buffer, 0, 1)) != -1) {
            bytes[index++] = buffer[0];
        }
        String output = new String(bytes).trim();
        assertEquals(data, output);
    }
}
