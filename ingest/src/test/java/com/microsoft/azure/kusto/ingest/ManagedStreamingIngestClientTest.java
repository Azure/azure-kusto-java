package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.microsoft.azure.kusto.ingest.StreamingIngestClientTest.jsonDataUncompressed;
import static com.microsoft.azure.kusto.ingest.StreamingIngestClientTest.verifyCompressedStreamContent;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ManagedStreamingIngestClientTest {
    private static final ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static final AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static IngestionProperties ingestionProperties;
    private static final String STORAGE_URL = "https://testcontosourl.com/storageUrl";

    @Mock
    private static StreamingClient streamingClientMock;

    @Captor
    private static ArgumentCaptor<InputStream> argumentCaptor;
    @Captor
    private static ArgumentCaptor<ClientRequestProperties> clientRequestPropertiesCaptor;

    private static final UUID CustomUUID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @BeforeAll
    static void setUp() throws Exception {
        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE))
                .thenReturn("queue1")
                .thenReturn("queue2");

        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE))
                .thenReturn("http://statusTable.com");

        when(resourceManagerMock.getIdentityToken()).thenReturn("identityToken");

        when(azureStorageClientMock.uploadStreamToBlob(any(InputStream.class), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI(STORAGE_URL)));

        when(azureStorageClientMock.getBlobPathWithSas(any(CloudBlockBlob.class))).thenReturn(STORAGE_URL);

        when(azureStorageClientMock.getBlobSize(anyString())).thenReturn(100L);

        when(azureStorageClientMock.uploadLocalFileToBlob(anyString(), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI(STORAGE_URL)));

        doNothing().when(azureStorageClientMock).azureTableInsertEntity(anyString(), any(TableServiceEntity.class));

        doNothing().when(azureStorageClientMock).postMessageToQueue(anyString(), anyString(), any());

        streamingClientMock = mock(StreamingClient.class);
        argumentCaptor = ArgumentCaptor.forClass((InputStream.class));
        clientRequestPropertiesCaptor = ArgumentCaptor.forClass(ClientRequestProperties.class);
    }

    @BeforeEach
    void setUpEach() throws IngestionServiceException, IngestionClientException {
        doReturn("storage1", "storage2").when(resourceManagerMock).getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE);

        ExponentialRetry retryTemplate = new ExponentialRetry(ManagedStreamingIngestClient.ATTEMPT_COUNT);
        retryTemplate.sleepBaseSecs = 0;
        retryTemplate.maxJitterSecs = 0;

        managedStreamingIngestClient = new ManagedStreamingIngestClient(resourceManagerMock, azureStorageClientMock, streamingClientMock,
                retryTemplate);
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.IngestionMappingKind.JSON);
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsQueue_IngestionStatusHardcoded() throws Exception {
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        IngestionResult result = managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assertEquals(1, result.getIngestionStatusesLength());
        assertEquals(OperationStatus.Queued, result.getIngestionStatusCollection().get(0).status);
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_NotEmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        IngestionResult result = managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assertNotEquals(0, result.getIngestionStatusesLength());
    }

    @Test
    void IngestFromBlob_NullIngestionProperties_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, null));
    }

    @Test
    void IngestFromBlob_NullBlobSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromBlob(null, ingestionProperties));
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_RemovesSecrets() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                "https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud",
                100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ArgumentCaptor<TableServiceEntity> capture = ArgumentCaptor.forClass(TableServiceEntity.class);

        managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).azureTableInsertEntity(anyString(), capture.capture());
        assert (((IngestionStatus) capture.getValue()).getIngestionSourcePath()).equals("https://storage.table.core.windows.net/ingestionsstatus20190505");
    }

    @Test
    void IngestFromFile_NullIngestionProperties_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromFile(fileSourceInfo, null));
    }

    @Test
    void IngestFromFile_NullFileSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromFile(null, ingestionProperties));
    }

    @Test
    void IngestFromFile_FileDoesNotExist_IngestionClientException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);
        assertThrows(
                IngestionClientException.class,
                () -> managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromStream_NullIngestionProperties_IllegalArgumentException() {
        StreamSourceInfo streamSourceInfo = mock(StreamSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromStream(streamSourceInfo, null));
    }

    @Test
    void IngestFromStream_NullStreamSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromStream(null, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_NullIngestionProperties_IllegalArgumentException() {
        ResultSetSourceInfo resultSetSourceInfo = mock(ResultSetSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromResultSet(resultSetSourceInfo, null));
    }

    @Test
    void IngestFromResultSet_NullResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> managedStreamingIngestClient.ingestFromResultSet(null, ingestionProperties));
    }

    // Since, like streamingClient, managedStreamingClient forwards everything to the IngestFromStream methods we can use similar tests
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void IngestFromFile_Csv(boolean useSourceId) throws Exception {
        UUID sourceId = useSourceId ? CustomUUID : null;
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length(), sourceId);
        OperationStatus status = managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));
        verifyClientRequestId(0, sourceId);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void IngestFromResultSet(boolean useSourceId) throws Exception {
        UUID sourceId = useSourceId ? CustomUUID : null;
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(1)).thenReturn("Name");
        when(resultSet.getObject(2)).thenReturn("Age");
        when(resultSet.getObject(3)).thenReturn("Weight");

        when(resultSetMetaData.getColumnCount()).thenReturn(3);

        ArgumentCaptor<InputStream> argumentCaptor = ArgumentCaptor.forClass(InputStream.class);

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.IngestionMappingKind.CSV);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet, sourceId);
        OperationStatus status = managedStreamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, "Name,Age,Weight");
        verifyClientRequestId(0, sourceId);
    }

    @Test
    void IngestFromFile_Json() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        String contents = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8).trim();

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status = managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                clientRequestPropertiesCaptor.capture(), any(String.class), any(String.class), any(boolean.class));
        verifyCompressedStreamContent(argumentCaptor.getValue(), contents);
        verifyClientRequestId();
    }

    @Test
    void IngestFromFile_CompressedJson() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json.gz";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        AtomicBoolean visited = new AtomicBoolean(false);

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);
        OperationStatus status;
        try {
            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    clientRequestPropertiesCaptor.capture(), any(String.class), any(String.class), any(boolean.class))).then(a -> {
                        verifyCompressedStreamContent(argumentCaptor.getValue(), jsonDataUncompressed);
                        visited.set(true);
                        return null;
                    });

            status = managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        } finally {
            reset(streamingClientMock);
        }
        assertEquals(OperationStatus.Succeeded, status);
        assertTrue(visited.get());
        verifyClientRequestId();
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,true", "true,false", "false,false"})
    void IngestFromStream_Success(boolean leaveOpen, boolean useSourceId) throws Exception {
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new CloseableByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        UUID sourceId = useSourceId ? CustomUUID : null;
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream, leaveOpen, sourceId);
        OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));

        /*
         * In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed. When the given
         * stream content is already compressed, the user must specify that in the stream source info. This method verifies if the stream was compressed
         * correctly.
         */
        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
        verifyClientRequestId(0, sourceId);
        if (leaveOpen) {
            assertDoesNotThrow(() -> inputStream.read(new byte[1]));
        } else {
            assertThrows(IOException.class, () -> {
                int _ignored = inputStream.read(new byte[1]);
            });
        }
    }

    @Test
    void IngestFromFile_Fail() throws Exception {
        try {
            // It's an array so we can safely modify it in the lambda
            final int[] times = {0};
            String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
            String path = resourcesDirectory + "testdata.json.gz";
            FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
            ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.JSON);

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    any(ClientRequestProperties.class), any(String.class), eq("JsonMapping"), any(boolean.class)))
                            .thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", false);
                            });

            // Should fail 3 times and then succeed with the queued client
            managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
            assertEquals(ManagedStreamingIngestClient.ATTEMPT_COUNT, times[0]);
        } finally {
            reset(streamingClientMock);
        }
    }

    @Test
    void IngestFromStream_FailStreaming() throws Exception {
        try {
            // It's an array, so we can safely modify it in the lambda
            final int[] times = {0};
            String data = "Name, Age, Weight, Height";
            InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    any(ClientRequestProperties.class), any(String.class), eq("mappingName"), any(boolean.class)))
                            .thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", false);
                            });

            // Should fail 3 times and then succeed with the queued client
            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
            managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
            assertEquals(ManagedStreamingIngestClient.ATTEMPT_COUNT, times[0]);
        } finally {
            reset(streamingClientMock);
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,true", "true,false", "false,false"})
    void IngestFromStream_FailFewTimes(boolean leaveOpen, boolean useSourceId) throws Exception {
        int failCount = 2;
        // It's an array so we can safely modify it in the lambda
        final int[] times = {0};
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new CloseableByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        UUID sourceId = useSourceId ? CustomUUID : null;

        try {
            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    any(ClientRequestProperties.class), any(String.class), eq("mappingName"), any(boolean.class)))
                            .thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", false);
                            }).thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", false);
                            }).thenReturn(null);

            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream, leaveOpen, sourceId);
            OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection()
                    .get(0).status;
            assertEquals(OperationStatus.Succeeded, status);
            assertEquals(failCount, times[0]);

            verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));
            InputStream stream = argumentCaptor.getValue();
            verifyCompressedStreamContent(stream, data);
            verifyClientRequestId(2, sourceId);

            if (leaveOpen) {
                assertDoesNotThrow(() -> inputStream.read(new byte[1]));
            } else {
                assertThrows(IOException.class, () -> {
                    int _ignored = inputStream.read(new byte[1]);
                });
            }
        } finally {
            reset(streamingClientMock);
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,true", "true,false", "false,false"})
    void IngestFromStream_FailTransientException(boolean leaveOpen, boolean useSourceId) throws Exception {
        try {
            int failCount = 2;
            // It's an array so we can safely modify it in the lambda
            final int[] times = {0};
            String data = "Name, Age, Weight, Height";
            InputStream inputStream = new CloseableByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
            DataWebException ex = new DataWebException("{\"error\" : {\n" +
                    "  \"code\": \"A\", \"message\": \"B\", \"@message\": \"C\", \"@type\": \"D\", \"@context\": {}, \n" +
                    "  \"@permanent\": false\n" +
                    "} }", null);
            UUID sourceId = useSourceId ? CustomUUID : null;

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    any(ClientRequestProperties.class), any(String.class), eq("mappingName"), any(boolean.class)))
                            .thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", ex, false);
                            }).thenAnswer((a) -> {
                                times[0]++;
                                throw new DataServiceException("some cluster", "Some error", ex, false);
                            }).thenReturn(null);

            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream, leaveOpen, sourceId);
            OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection()
                    .get(0).status;
            assertEquals(OperationStatus.Succeeded, status);
            assertEquals(failCount, times[0]);

            verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));
            InputStream stream = argumentCaptor.getValue();
            verifyCompressedStreamContent(stream, data);
            verifyClientRequestId(2, sourceId);
            if (leaveOpen) {
                assertDoesNotThrow(() -> inputStream.read(new byte[1]));
            } else {
                assertThrows(IOException.class, () -> {
                    int _ignored = inputStream.read(new byte[1]);
                });
            }
        } finally {
            reset(streamingClientMock);
        }
    }

    @Test
    void IngestFromStream_FailPermanentException() throws Exception {
        try {
            // It's an array, so we can safely modify it in the lambda
            String data = "Name, Age, Weight, Height";
            InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
            DataWebException ex = new DataWebException("{\"error\" : {\n" +
                    "  \"code\": \"A\", \"message\": \"B\", \"@message\": \"C\", \"@type\": \"D\", \"@context\": {}, \n" +
                    "  \"@permanent\": true\n" +
                    "} }", null);

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    any(ClientRequestProperties.class), any(String.class), eq("mappingName"), any(boolean.class)))
                            .thenAnswer((a) -> {
                                throw new DataServiceException("some cluster", "Some error", ex, true);
                            }).thenAnswer((a) -> {
                                throw new DataServiceException("some cluster", "Some error", ex, true);
                            }).thenReturn(null);
            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
            assertThrows(IngestionServiceException.class, () -> managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties));
        } finally {
            reset(streamingClientMock);
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,true", "true,false", "false,false"})
    void IngestFromStream_IngestOverFileLimit_QueuedFallback(boolean leaveOpen, boolean useSourceId) throws Exception {
        int testByteArraySize = 5 * 1024 * 1024;
        byte[] bytes = new byte[testByteArraySize];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }

        UUID sourceId = useSourceId ? CustomUUID : null;
        InputStream inputStream = new CloseableByteArrayInputStream(bytes);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);

        ArgumentCaptor<InputStream> capture = ArgumentCaptor.forClass(InputStream.class);

        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream, leaveOpen, sourceId);
        managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);

        verify(streamingClientMock, never()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                clientRequestPropertiesCaptor.capture(), any(String.class), eq("mappingName"), any(boolean.class));

        verify(azureStorageClientMock, atLeast(1)).uploadStreamToBlob(capture.capture(), anyString(), anyString(), anyBoolean());

        InputStream value = capture.getValue();
        if (leaveOpen) {
            assertArrayEquals(bytes, IngestionUtils.readBytesFromInputStream(value, testByteArraySize));
        } else {
            assertThrows(IOException.class, () -> {
                int _ignored = inputStream.read(new byte[1]);
            });
        }
    }

    @Test
    void CreateManagedStreamingIngestClient_WithDefaultCtor_WithQueryUri_Pass() throws URISyntaxException {
        ManagedStreamingIngestClient client = IngestClientFactory.createManagedStreamingIngestClient(ConnectionStringBuilder.createWithUserPrompt("https" +
                "://testendpoint.dev.kusto.windows.net"));
        assertNotNull(client);
        assertEquals("https://ingest-testendpoint.dev.kusto.windows.net", client.queuedIngestClient.connectionDataSource);
        assertEquals("https://testendpoint.dev.kusto.windows.net", client.streamingIngestClient.connectionDataSource);
    }

    @Test
    void CreateManagedStreamingIngestClient_WithDefaultCtor_WithIngestUri_Pass() throws URISyntaxException {
        ManagedStreamingIngestClient client = IngestClientFactory.createManagedStreamingIngestClient(ConnectionStringBuilder.createWithUserPrompt("https" +
                "://ingest-testendpoint.dev.kusto.windows.net"));
        assertNotNull(client);
        assertEquals("https://ingest-testendpoint.dev.kusto.windows.net", client.queuedIngestClient.connectionDataSource);
        assertEquals("https://testendpoint.dev.kusto.windows.net", client.streamingIngestClient.connectionDataSource);
    }

    @Test
    void CreateManagedStreamingIngestClient_WithDmUri_Pass() throws URISyntaxException {
        ManagedStreamingIngestClient client = ManagedStreamingIngestClient
                .fromDmConnectionString(ConnectionStringBuilder.createWithUserPrompt("https://ingest-testendpoint.dev.kusto.windows.net"));
        assertNotNull(client);
        assertEquals("https://ingest-testendpoint.dev.kusto.windows.net", client.queuedIngestClient.connectionDataSource);
        assertEquals("https://testendpoint.dev.kusto.windows.net", client.streamingIngestClient.connectionDataSource);
    }

    @Test
    void CreateManagedStreamingIngestClient_WithEngineUri_Pass() throws URISyntaxException {
        ManagedStreamingIngestClient client = ManagedStreamingIngestClient.fromEngineConnectionString(
                ConnectionStringBuilder.createWithUserPrompt("https://testendpoint.dev.kusto.windows.net"));
        assertNotNull(client);
        assertEquals("https://ingest-testendpoint.dev.kusto.windows.net", client.queuedIngestClient.connectionDataSource);
        assertEquals("https://testendpoint.dev.kusto.windows.net", client.streamingIngestClient.connectionDataSource);
    }

    private static void verifyClientRequestId() {
        verifyClientRequestId(0, null);
    }

    private static void verifyClientRequestId(int count, @Nullable UUID expectedUUID) {
        String clientRequestId = clientRequestPropertiesCaptor.getValue().getClientRequestId();
        assertNotNull(clientRequestId);
        String[] values = clientRequestId.split(";");
        assertEquals(3, values.length);
        assertEquals("KJC.executeManagedStreamingIngest", values[0]);
        assertDoesNotThrow(() -> {
            UUID actual = UUID.fromString(values[1]);
            if (expectedUUID != null) {
                assertEquals(expectedUUID, actual);
            }
        });
        assertDoesNotThrow(() -> assertEquals(Integer.parseInt(values[2]), count));
    }
}
