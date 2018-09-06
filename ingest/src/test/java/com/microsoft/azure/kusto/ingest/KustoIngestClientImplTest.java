package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class KustoIngestClientImplTest {

    ResourceManager resourceManagerMock = mock(ResourceManager.class);
    KustoIngestClient ingestClientMock;
    KustoIngestClientImpl ingestClientMockImpl;
    KustoIngestionProperties props;

    @BeforeEach
    void setUp() {
        try {
            ingestClientMock = mock(KustoIngestClient.class);
            ingestClientMockImpl = mock(KustoIngestClientImpl.class);

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceTypes.SECURED_READY_FOR_AGGREGATION_QUEUE))
                    .thenReturn("queue1")
                    .thenReturn("queue2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceTypes.TEMP_STORAGE))
                    .thenReturn("storage1")
                    .thenReturn("storage2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceTypes.INGESTIONS_STATUS_TABLE))
                    .thenReturn("statusTable");

            when(resourceManagerMock.getKustoIdentityToken())
                    .thenReturn("identityToken");

            props = new KustoIngestionProperties("dbName", "tableName");
            props.setJsonMappingName("mappingName");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void ingestFromBlob() {
        try {
            doReturn(null).when(ingestClientMock).ingestFromBlob(isA(BlobSourceInfo.class), isA(KustoIngestionProperties.class));

            String blobPath = "blobPath";
            long size = 100;

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, size);

            ingestClientMock.ingestFromBlob(blobSourceInfo, props);

            verify(ingestClientMock).ingestFromBlob(blobSourceInfo, props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromFile() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(ingestClientMockImpl.uploadLocalFileToBlob(isA(String.class), isA(String.class), isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(ingestClientMockImpl).postMessageToQueue(isA(String.class), isA(String.class));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                ingestClientMock.ingestFromFile(fileSourceInfo, props);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromFile(fileSourceInfo, props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStream() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(ingestClientMockImpl.uploadStreamToBlob(isA(InputStream.class), isA(String.class), isA(String.class), isA(Boolean.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));
            doNothing().when(ingestClientMockImpl).postMessageToQueue(isA(String.class), isA(String.class));
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                InputStream stream = new FileInputStream(testFilePath);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
                ingestClientMock.ingestFromStream(streamSourceInfo, props);
            }
            verify(ingestClientMock, times(numOfFiles)).ingestFromStream(any(StreamSourceInfo.class), any(KustoIngestionProperties.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
