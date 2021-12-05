package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.StreamingClient;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.microsoft.azure.kusto.ingest.StreamingIngestClientTest.jsonDataUncompressed;
import static com.microsoft.azure.kusto.ingest.StreamingIngestClientTest.verifyCompressedStreamContent;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ManagedStreamingIngestClientTest {
    private static final ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static final AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static IngestionProperties ingestionProperties;
    private static final String STORAGE_URI = "https://ms.com/storageUri";

    @Mock
    private static StreamingClient streamingClientMock;

    @Captor
    private static ArgumentCaptor<InputStream> argumentCaptor;

    @BeforeAll
    static void setUp() throws Exception {
        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE))
                .thenReturn("queue1")
                .thenReturn("queue2");

        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE))
                .thenReturn("http://statusTable.com");

        when(resourceManagerMock.getIdentityToken()).thenReturn("identityToken");

        when(azureStorageClientMock.uploadStreamToBlob(any(InputStream.class), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI(STORAGE_URI)));

        when(azureStorageClientMock.getBlobPathWithSas(any(CloudBlockBlob.class))).thenReturn(STORAGE_URI);

        when(azureStorageClientMock.getBlobSize(anyString())).thenReturn(100L);

        when(azureStorageClientMock.uploadLocalFileToBlob(anyString(), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI(STORAGE_URI)));

        doNothing().when(azureStorageClientMock).azureTableInsertEntity(anyString(), any(TableServiceEntity.class));

        doNothing().when(azureStorageClientMock).postMessageToQueue(anyString(), anyString());

        streamingClientMock = mock(StreamingClient.class);
        argumentCaptor = ArgumentCaptor.forClass((InputStream.class));
    }

    @BeforeEach
    void setUpEach() throws IngestionServiceException, IngestionClientException {
        doReturn("storage1", "storage2").when(resourceManagerMock).getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE);

        managedStreamingIngestClient = new ManagedStreamingIngestClient(resourceManagerMock, azureStorageClientMock, streamingClientMock);
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.IngestionMappingKind.Json);
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsQueue_IngestionStatusHardcoded1() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        IngestionResult result = managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assertEquals(1, result.getIngestionStatusesLength());
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_NotEmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);

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
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
        ArgumentCaptor<TableServiceEntity> captur = ArgumentCaptor.forClass(TableServiceEntity.class);

        managedStreamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).azureTableInsertEntity(anyString(), captur.capture());
        assert (((IngestionStatus) captur.getValue()).getIngestionSourcePath()).equals("https://storage.table.core.windows.net/ingestionsstatus20190505");
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

    //Since, like streamingClient, managedStreamingClient forwards everything to the IngestFromStream methods we can use similar tests
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
        OperationStatus status = managedStreamingIngestClient.ingestFromResultSet(resultSetSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), eq("mappingName"), any(boolean.class));

        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, "Name,Age,Weight");
    }

    @Test
    void IngestFromFile_Csv() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        OperationStatus status = managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), eq("mappingName"), any(boolean.class));
    }

    @Test
    void IngestFromFile_Json() throws Exception {
        String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
        String path = resourcesDirectory + "testdata.json";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        String contents = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8).trim();

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status = managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
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
        AtomicBoolean visited = new AtomicBoolean(false);

        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);
        OperationStatus status;
        try {
            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), any(String.class), any(boolean.class))).then(a -> {
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
    }

    @Test
    void IngestFromStream_Success() throws Exception {
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);
        verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                isNull(), any(String.class), eq("mappingName"), any(boolean.class));

        /* In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed.
         * When the given stream content is already compressed, the user must specify that in the stream source info.
         * This method verifies if the stream was compressed correctly.
         */
        InputStream stream = argumentCaptor.getValue();
        verifyCompressedStreamContent(stream, data);
    }

    @Test
    void IngestFromFile_Fail() throws Exception {
        try {
            // It's an array so we can safely modify it in the lambda
            final int[] times = {0};
            String resourcesDirectory = System.getProperty("user.dir") + "/src/test/resources/";
            String path = resourcesDirectory + "testdata.json.gz";
            FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
            ingestionProperties.setDataFormat(IngestionProperties.DataFormat.json);
            ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.IngestionMappingKind.Json);

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), eq("JsonMapping"), any(boolean.class)))
                    .thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", false);
                    });

            // Should fail 3 times and then succeed with the queued client
            managedStreamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
            assertEquals(ManagedStreamingIngestClient.MAX_RETRY_CALLS, times[0]);
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
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class)))
                    .thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", false);
                    });

            // Should fail 3 times and then succeed with the queued client
            managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
            assertEquals(ManagedStreamingIngestClient.MAX_RETRY_CALLS, times[0]);
        } finally {
            reset(streamingClientMock);
        }
    }

    @Test
    void IngestFromStream_FailFewTimes() throws Exception {
        int failCount = 2;
        // It's an array so we can safely modify it in the lambda
        final int[] times = {0};
        String data = "Name, Age, Weight, Height";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());

        try {
            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class)))
                    .thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", false);
                    }).thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", false);
                    }).thenReturn(null);

            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
            OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
            assertEquals(OperationStatus.Succeeded, status);
            assertEquals(failCount, times[0]);

            verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class));
            InputStream stream = argumentCaptor.getValue();
            verifyCompressedStreamContent(stream, data);
        } finally {
            reset(streamingClientMock);
        }
    }

    @Test
    void IngestFromStream_FailTransientException() throws Exception {
        try {
            int failCount = 2;
            // It's an array so we can safely modify it in the lambda
            final int[] times = {0};
            String data = "Name, Age, Weight, Height";
            InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
            DataWebException ex = new DataWebException("{\"error\" : {\n" +
                    "  \"code\": \"A\", \"message\": \"B\", \"@message\": \"C\", \"@type\": \"D\", \"@context\": {}, \n" +
                    "  \"@permanent\": false\n" +
                    "} }", null);

            when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class)))
                    .thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", ex, false);
                    }).thenAnswer((a) -> {
                        times[0]++;
                        throw new DataServiceException("some cluster", "Some error", ex, false);
                    }).thenReturn(null);

            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
            OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
            assertEquals(OperationStatus.Succeeded, status);
            assertEquals(failCount, times[0]);

            verify(streamingClientMock, atLeastOnce()).executeStreamingIngest(any(String.class), any(String.class), argumentCaptor.capture(),
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class));
            InputStream stream = argumentCaptor.getValue();
            verifyCompressedStreamContent(stream, data);
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
                    isNull(), any(String.class), eq("mappingName"), any(boolean.class)))
                    .thenAnswer((a) -> {
                        throw new DataServiceException("some cluster", "Some error", ex, true);
                    }).thenAnswer((a) -> {
                        throw new DataServiceException("some cluster", "Some error", ex, true);
                    }).thenReturn(null);
            StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
            assertThrows(IngestionServiceException.class, () -> managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties));
        } finally {
            reset(streamingClientMock);
        }
    }
}