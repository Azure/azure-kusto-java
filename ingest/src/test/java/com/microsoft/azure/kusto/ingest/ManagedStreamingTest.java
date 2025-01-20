package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.kusto.data.ExponentialRetry;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.commons.lang3.function.BooleanConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ManagedStreamingTest {
    private static final ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static final AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    public static final String ACCOUNT_NAME = "someaccount";
    private static QueuedIngestClient queuedIngestClientMock;
    private static IngestionProperties ingestionProperties;
    private static StreamingClient streamingClientMock;
    private static KustoOperationResult kustoOperationResultMock;
    private static ManagedStreamingIngestClient managedStreamingIngestClient;
    private static ManagedStreamingIngestClient managedStreamingIngestClientSpy;

    @BeforeAll
    static void setUp() {
        kustoOperationResultMock = mock(KustoOperationResult.class);
        when(resourceManagerMock.getShuffledContainers())
                .thenReturn(Collections.singletonList(TestUtils.containerWithSasFromAccountNameAndContainerName(ACCOUNT_NAME, "someStorage")));
        when(resourceManagerMock.getShuffledQueues())
                .thenReturn(Collections.singletonList(TestUtils.queueWithSasFromAccountNameAndQueueName(ACCOUNT_NAME, "someQueue")));

        when(resourceManagerMock.getStatusTable())
                .thenReturn(TestUtils.tableWithSasFromTableName("http://statusTable.com"));

        when(resourceManagerMock.getIdentityToken()).thenReturn("identityToken");

        when(azureStorageClientMock.azureTableInsertEntity(any(), any(TableEntity.class))).thenReturn(Mono.empty());

        when(azureStorageClientMock.postMessageToQueue(any(), anyString())).thenReturn(Mono.empty());
        when(azureStorageClientMock.uploadStreamToBlob(any(), any(), any(), anyBoolean())).thenReturn(Mono.empty());
        streamingClientMock = mock(StreamingClient.class);
        when(streamingClientMock.executeStreamingIngestAsync(any(), any(), any(),
                any(), any(), any(), any(boolean.class))).thenReturn(Mono.just(kustoOperationResultMock));

        ingestionProperties = new IngestionProperties("dbName", "tableName");
        managedStreamingIngestClient = new ManagedStreamingIngestClient(resourceManagerMock, azureStorageClientMock,
                streamingClientMock);
        queuedIngestClientMock = mock(QueuedIngestClientImpl.class);
        managedStreamingIngestClientSpy = spy(
                new ManagedStreamingIngestClient(mock(StreamingIngestClient.class), queuedIngestClientMock, new ExponentialRetry(1)));
    }

    static ByteArrayInputStream createStreamOfSize(int size) throws UnsupportedEncodingException {
        char[] charArray = new char[size];
        Arrays.fill(charArray, 'a');
        String str = new String(charArray);
        byte[] byteArray = str.getBytes("UTF-8");
        return new ByteArrayInputStream(byteArray);
    }

    static int getStreamSize(InputStream inputStream) throws IOException {
        int size = 0;
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            size += bytesRead;
        }
        return size;
    }

    @Test
    void ingestFromStream_CsvStream() throws Exception {

        InputStream inputStream = createStreamOfSize(1);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);

        // Expect to work and also choose no queuing
        OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection()
                .block().get(0).status;
        assertEquals(OperationStatus.Succeeded, status);

        BooleanConsumer assertPolicyWorked = (boolean wasExpectedToUseQueuing) -> {
            try {
                inputStream.reset();
                IngestionStatus ingestionStatus = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties)
                        .getIngestionStatusCollection().block().get(0);
                if (wasExpectedToUseQueuing) {
                    assertEquals(OperationStatus.Queued, ingestionStatus.status);
                } else {
                    assertEquals(OperationStatus.Succeeded, ingestionStatus.status);
                }
                System.out.println(ingestionStatus.status);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // if size was given - it should be used against MAX_STREAMING_RAW_SIZE_BYTES
        streamSourceInfo.setRawSizeInBytes(ManagedStreamingQueuingPolicy.MAX_STREAMING_RAW_SIZE_BYTES + 1);
        assertPolicyWorked.accept(true);

        streamSourceInfo.setRawSizeInBytes(ManagedStreamingQueuingPolicy.MAX_STREAMING_RAW_SIZE_BYTES - 1);
        assertPolicyWorked.accept(false);
    }

    @Test
    void shouldUseQueueingPredicate_DefaultBehavior() {
        // Raw data size is set - choose queuing although data is small
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(
                1, ManagedStreamingQueuingPolicy.MAX_STREAMING_RAW_SIZE_BYTES + 1, false, IngestionProperties.DataFormat.CSV));

        // CSV uncompressed - allow big file
        int bigFile = 7 * 1024 * 1024;
        assertFalse(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(bigFile,
                0, false, IngestionProperties.DataFormat.CSV));

        // CSV compressed - don't allow big files
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(bigFile,
                0, true, IngestionProperties.DataFormat.CSV));
        int mediumSizeCompressed = 3 * 1024 * 1024;
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(mediumSizeCompressed,
                0, true, IngestionProperties.DataFormat.CSV));

        int smallCompressed = 2 * 1024 * 1024;
        assertFalse(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(smallCompressed,
                0, true, IngestionProperties.DataFormat.CSV));

        // JSON uncompress- allow big file
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(bigFile,
                0, false, IngestionProperties.DataFormat.JSON));

        // JSON compressed
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(mediumSizeCompressed,
                0, true, IngestionProperties.DataFormat.JSON));
        assertFalse(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(smallCompressed,
                0, true, IngestionProperties.DataFormat.JSON));

        // AVRO - either compressed or not do not allow medium
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(mediumSizeCompressed,
                0, true, IngestionProperties.DataFormat.AVRO));
        assertTrue(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(mediumSizeCompressed,
                0, false, IngestionProperties.DataFormat.AVRO));

        // AVRO - either compressed or not allow small
        assertFalse(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(smallCompressed,
                0, true, IngestionProperties.DataFormat.AVRO));
        assertFalse(ManagedStreamingQueuingPolicy.Default.shouldUseQueuedIngestion(smallCompressed,
                0, false, IngestionProperties.DataFormat.AVRO));
    }

    @Test
    void ManagedStreaming_BigFile_ShouldQueueTheFullStream() throws IOException, IngestionClientException, IngestionServiceException {
        EmptyAvailableByteArrayOutputStream inputStream = new EmptyAvailableByteArrayOutputStream(
                createStreamOfSize(ManagedStreamingQueuingPolicy.MAX_STREAMING_STREAM_SIZE_BYTES + 10));
        int size = inputStream.bb.available();
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ArgumentCaptor<StreamSourceInfo> streamSourceInfoCaptor = ArgumentCaptor.forClass(StreamSourceInfo.class);
        when(queuedIngestClientMock.ingestFromStreamAsync(any(), any())).thenReturn(Mono.empty());
        managedStreamingIngestClientSpy.ingestFromStream(streamSourceInfo, ingestionProperties);
        verify(queuedIngestClientMock).ingestFromStreamAsync(streamSourceInfoCaptor.capture(), any());

        StreamSourceInfo value = streamSourceInfoCaptor.getValue();
        int queuedStreamSize = getStreamSize(value.getStream());
        Assertions.assertEquals(queuedStreamSize, size);
    }

    static class EmptyAvailableByteArrayOutputStream extends InputStream {
        private ByteArrayInputStream bb;

        EmptyAvailableByteArrayOutputStream(ByteArrayInputStream bb) {
            this.bb = bb;
        }

        @Override
        public int read() {
            return bb.read();
        }

        @Override
        public synchronized int available() {
            return 0;
        }
    }
}
