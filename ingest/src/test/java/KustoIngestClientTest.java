import com.microsoft.azure.kusto.ingest.KustoBatchIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestionProperties;
import com.microsoft.azure.kusto.ingest.ResourceManager;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class KustoIngestClientTest {

    ResourceManager resourceManagerMock = mock(ResourceManager.class);
    KustoIngestClient ingestClientMock;
    KustoBatchIngestClient batchIngestClientMock;
    KustoIngestionProperties props;

    @BeforeEach
     void setUp() {
        try {
            ingestClientMock = mock(KustoIngestClient.class);
            batchIngestClientMock = mock(KustoBatchIngestClient.class);

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
            String testFilePath = Paths.get("src","test","resources","testdata.json").toString();

            when(batchIngestClientMock.uploadLocalFileToBlob(isA(String.class),isA(String.class),isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(batchIngestClientMock).postMessageToQueue(isA(String.class),isA(String.class));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath,0);
            int numOfFiles = 3;
            for(int i=0; i<numOfFiles; i++){
                ingestClientMock.ingestFromFile(fileSourceInfo,props);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromFile(fileSourceInfo,props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}