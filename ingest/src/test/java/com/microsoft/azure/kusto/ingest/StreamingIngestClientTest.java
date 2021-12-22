// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.microsoft.azure.kusto.ingest.IngestClientBase.WRONG_ENDPOINT_MESSAGE;
import static com.microsoft.azure.kusto.ingest.StreamingIngestClient.EXPECTED_SERVICE_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StreamingIngestClientTest {

    private static StreamingIngestClient streamingIngestClient;
    private IngestionProperties ingestionProperties;

    @Mock
    private static StreamingClient streamingClientMock;

    @Captor
    private static ArgumentCaptor<InputStream> argumentCaptor;

    private static final String ENDPOINT_SERVICE_TYPE_DM = "DataManagement";


    @BeforeAll
    static void setUp() {
        streamingClientMock = mock(StreamingClient.class);
        streamingIngestClient = new StreamingIngestClient(streamingClientMock);
        argumentCaptor = ArgumentCaptor.forClass((InputStream.class));
    }

    @BeforeEach
    void setUpEach() throws Exception {
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.csv);
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class))).thenReturn(null);

        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class))).thenReturn(null);
    }

    @Test
    void IngestFromStream_CsvStream() throws Exception {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
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
        byte[] inputArray = StandardCharsets.UTF_8.encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        // When ingesting compressed data, we should set this property true to avoid double compression.
        streamSourceInfo.setCompressionType(CompressionType.gz);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), isNull(), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromStream_JsonStream() throws Exception {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
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
        byte[] inputArray = StandardCharsets.UTF_8.encode(data).array();
        gzipOutputStream.write(inputArray, 0, inputArray.length);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        streamSourceInfo.setCompressionType(CompressionType.gz);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
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
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, null),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithNullDatabase_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties(null, "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithEmptyDatabase_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("", "table");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithNullTable_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("database", null);
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_IngestionPropertiesWithEmptyTable_IllegalArgumentException() {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties = new IngestionProperties("database", "");
        assertThrows(IllegalArgumentException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IllegalArgumentException to be thrown, but it didn't");
    }

    @Test
    void IngestFromStream_JsonNoMappingReference_IngestionClientException() {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for json format."));
    }

    @Test
    void IngestFromStream_JsonWrongMappingKind_IngestionClientException() {
        String data = "{\"Name\": \"name\", \"Age\": \"age\", \"Weight\": \"weight\", \"Height\": \"height\"}";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.Csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found Csv mapping kind."));
    }

    @Test
    void IngestFromStream_AvroNoMappingReference_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.avro);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Mapping reference must be specified for avro format."));
    }

    @Test
    void IngestFromStream_AvroWrongMappingKind_IngestionClientException() {
        InputStream inputStream = new ByteArrayInputStream(new byte[10]);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.avro);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.Csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format avro, found Csv mapping kind."));
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
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
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
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
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
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), isNull(), any(boolean.class));
    }

    @Test
    void IngestFromFile_GivenStreamingIngestClientAndDmEndpoint_ThrowsIngestionClientException() throws Exception {
        DataServiceException dataClientException = new DataServiceException("some cluster", "Error in post request. status 404",
                new DataWebException("Error in post request", null), true);
        doThrow(dataClientException).when(streamingClientMock).executeStreamingIngest(eq(ingestionProperties.getDatabaseName()), eq(ingestionProperties.getTableName()), any(), isNull(), any(), isNull(), eq(false));
        when(streamingClientMock.execute(Commands.VERSION_SHOW_COMMAND)).thenReturn(new KustoOperationResult("{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"BuildVersion\",\"DataType\":\"String\"},{\"ColumnName\":\"BuildTime\",\"DataType\":\"DateTime\"},{\"ColumnName\":\"ServiceType\",\"DataType\":\"String\"},{\"ColumnName\":\"ProductVersion\",\"DataType\":\"String\"}],\"Rows\":[[\"1.0.0.0\",\"2000-01-01T00:00:00Z\",\"DataManagement\",\"PrivateBuild.yischoen.YISCHOEN-OP7070.2020-09-07 12-09-22\"]]}]}", "v1"));

        streamingIngestClient.setConnectionDataSource("https://ingest-testendpoint.dev.kusto.windows.net");
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        String expectedMessage =
                String.format(WRONG_ENDPOINT_MESSAGE + ": '%s'", EXPECTED_SERVICE_TYPE, ENDPOINT_SERVICE_TYPE_DM, "https://testendpoint.dev.kusto.windows.net");
        Exception exception = assertThrows(IngestionClientException.class, () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties));
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    void IngestFromFile_Json() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        String contents = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8).trim();

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        verifyCompressedStreamContent(argumentCaptor.getValue(), contents);
    }

    @Test
    void IngestFromFile_CompressedJson() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json.gz";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), any(String.class), any(boolean.class));

        verifyCompressedStreamContent(argumentCaptor.getValue(), jsonDataUncompressed);
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
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
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
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("CsvMapping", IngestionMapping.IngestionMappingKind.Csv);
        IngestionClientException ingestionClientException = assertThrows(IngestionClientException.class,
                () -> streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties),
                "Expected IngestionClientException to be thrown, but it didn't");
        assertTrue(ingestionClientException.getMessage().contains("Wrong ingestion mapping for format json, found Csv mapping kind."));
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
        assertEquals(OperationStatus.Succeeded, status);
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
        assertEquals(OperationStatus.Succeeded, status);
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
    public static void verifyCompressedStreamContent(InputStream compressedStream, String data) throws Exception {
        GZIPInputStream gzipInputStream = new GZIPInputStream(compressedStream);
        byte[] buffer = new byte[1];
        byte[] bytes = new byte[4096];
        int index = 0;
        while ((gzipInputStream.read(buffer, 0, 1)) != -1) {
            bytes[index++] = buffer[0];
        }
        String output = new String(bytes).trim();

        assertEquals(data, output);
    }

    public static String jsonDataUncompressed = "{\"Name\":\"demo1\",\"Code\":\"091231\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"091232\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"091233\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"091234\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"091235\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"091236\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"091237\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"091238\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"091239\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"091230\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"0912311\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"0912322\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"0912333\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"0912344\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"0912355\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"0912366\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"0912377\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"0912388\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"0912399\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"0912300\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"0912113\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"0912223\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"0912333\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"0912443\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"0912553\"}\n" +
            "{\"Name\":\"demo15\",\"Code\":\"0912663\"}\n" +
            "{\"Name\":\"demo16\",\"Code\":\"0912773\"}\n" +
            "{\"Name\":\"demo17\",\"Code\":\"0912883\"}\n" +
            "{\"Name\":\"demo18\",\"Code\":\"0912399\"}\n" +
            "{\"Name\":\"demo19\",\"Code\":\"0912003\"}\n" +
            "{\"Name\":\"demo10\",\"Code\":\"091231\"}\n" +
            "{\"Name\":\"demo11\",\"Code\":\"091232\"}\n" +
            "{\"Name\":\"demo12\",\"Code\":\"091233\"}\n" +
            "{\"Name\":\"demo13\",\"Code\":\"091234\"}\n" +
            "{\"Name\":\"demo14\",\"Code\":\"091235\"}";
}
