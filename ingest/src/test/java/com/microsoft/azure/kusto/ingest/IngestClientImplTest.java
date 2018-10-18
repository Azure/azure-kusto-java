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
    private IngestClientImpl ingestClientImplMock = mock(IngestClientImpl.class);;
    private AzureStorageHelper azureStorageHelperMock = mock(AzureStorageHelper.class);
    private IngestionProperties props;

    @BeforeEach
    void setUp() {
        try {
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

            doNothing().when(azureStorageHelperMock).postMessageToQueue(isA(String.class),isA(String.class));

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
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo("blobPath", 100);

            ingestClientImplMock.ingestFromBlob(blobSourceInfo, props);
            verify(ingestClientImplMock).ingestFromBlob(any(BlobSourceInfo.class), any(IngestionProperties.class));

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

    @Test
    void ingestFromBlobAsync() {
        try{
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo("blobPath", 100);
            ingestClientImplMock.ingestFromBlobAsync(blobSourceInfo, props);
            verify(ingestClientImplMock).ingestFromBlobAsync(any(BlobSourceInfo.class), any(IngestionProperties.class));

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromFileAsync() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(azureStorageHelperMock.uploadLocalFileToBlob(isA(String.class), isA(String.class), isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                ingestClientImplMock.ingestFromFileAsync(fileSourceInfo, props);
            }
            verify(ingestClientImplMock, times(numOfFiles)).ingestFromFileAsync(any(FileSourceInfo.class), any(IngestionProperties.class));

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStreamAsync() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(azureStorageHelperMock.uploadStreamToBlob(isA(InputStream.class), isA(String.class), isA(String.class), isA(Boolean.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                InputStream stream = new FileInputStream(testFilePath);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
                ingestClientImplMock.ingestFromStreamAsync(streamSourceInfo, props);
            }
            verify(ingestClientImplMock, times(numOfFiles)).ingestFromStreamAsync(any(StreamSourceInfo.class), any(IngestionProperties.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
