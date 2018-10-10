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

class IngestClientImplTest {

    private ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private IngestClientImpl ingestClientImplMock;
    private AzureStorageHelper azureStorageHelperMock;
    private IngestionProperties props;

    @BeforeEach
    void setUp() {
        try {
            ingestClientImplMock = mock(IngestClientImpl.class);
            azureStorageHelperMock = mock(AzureStorageHelper.class);

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE))
                    .thenReturn("queue1")
                    .thenReturn("queue2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE))
                    .thenReturn("storage1")
                    .thenReturn("storage2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE))
                    .thenReturn("statusTable");

            when(resourceManagerMock.getIdentityToken())
                    .thenReturn("identityToken");

            props = new IngestionProperties("dbName", "tableName");
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
            doReturn(null).when(ingestClientImplMock).ingestFromBlob(isA(BlobSourceInfo.class), isA(IngestionProperties.class));

            String blobPath = "blobPath";
            long size = 100;

            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, size);

            ingestClientImplMock.ingestFromBlob(blobSourceInfo, props);

            verify(ingestClientImplMock).ingestFromBlob(blobSourceInfo, props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromFile() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(azureStorageHelperMock.uploadLocalFileToBlob(isA(String.class), isA(String.class), isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(azureStorageHelperMock).postMessageToQueue(isA(String.class), isA(String.class));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                ingestClientImplMock.ingestFromFile(fileSourceInfo, props);
            }

            verify(ingestClientImplMock, times(numOfFiles)).ingestFromFile(fileSourceInfo, props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStream() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(azureStorageHelperMock.uploadStreamToBlob(isA(InputStream.class), isA(String.class), isA(String.class), isA(Boolean.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));
            doNothing().when(azureStorageHelperMock).postMessageToQueue(isA(String.class), isA(String.class));
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                InputStream stream = new FileInputStream(testFilePath);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
                ingestClientImplMock.ingestFromStream(streamSourceInfo, props);
            }
            verify(ingestClientImplMock, times(numOfFiles)).ingestFromStream(any(StreamSourceInfo.class), any(IngestionProperties.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
