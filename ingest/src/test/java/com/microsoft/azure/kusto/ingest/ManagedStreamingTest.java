package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.commons.lang3.function.BooleanConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class ManagedStreamingTest {
    private static final ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static final AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    public static final String ACCOUNT_NAME = "someaccount";
    private static QueuedIngestClient queuedIngestClient;
    private static IngestionProperties ingestionProperties;
    private static StreamingClient streamingClientMock;

    @BeforeAll
    static void setUp() throws Exception {
        when(resourceManagerMock.getShuffledContainers())
                .thenReturn(Collections.singletonList(TestUtils.containerWithSasFromAccountNameAndContainerName(ACCOUNT_NAME, "someStorage")));
        when(resourceManagerMock.getShuffledQueues())
                .thenReturn(Collections.singletonList(TestUtils.queueWithSasFromAccountNameAndQueueName(ACCOUNT_NAME, "someQueue")));

        when(resourceManagerMock.getStatusTable())
                .thenReturn(TestUtils.tableWithSasFromTableName("http://statusTable.com"));

        when(resourceManagerMock.getIdentityToken()).thenReturn("identityToken");

        doNothing().when(azureStorageClientMock).azureTableInsertEntity(any(), any(TableEntity.class));

        doNothing().when(azureStorageClientMock).postMessageToQueue(any(), anyString());
        streamingClientMock = mock(StreamingClient.class);
        when(streamingClientMock.executeStreamingIngest(any(String.class), any(String.class), any(InputStream.class),
                isNull(), any(String.class), any(String.class), any(boolean.class))).thenReturn(null);

        ingestionProperties = new IngestionProperties("dbName", "tableName");
    }

    static InputStream createStreamOfSize(int size) {
        char[] charArray = new char[size];
        Arrays.fill(charArray, 'a');
        String str = new String(charArray);
        return new ByteArrayInputStream(StandardCharsets.UTF_8.encode(str).array());
    }

    @Test
    void IngestFromStream_CsvStream() throws Exception {

        InputStream inputStream = createStreamOfSize(1);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);

        ManagedStreamingIngestClient managedStreamingIngestClient = new ManagedStreamingIngestClient(resourceManagerMock, azureStorageClientMock,
                streamingClientMock);

        // Expect to work and also choose no queuing
        OperationStatus status = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection()
                .get(0).status;
        assertEquals(OperationStatus.Succeeded, status);

        BooleanConsumer assertPolicyWorked = (boolean wasExpectedToUseQueuing) -> {
            try {
                inputStream.reset();
                IngestionStatus ingestionStatus = managedStreamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties)
                        .getIngestionStatusCollection().get(0);
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
}
