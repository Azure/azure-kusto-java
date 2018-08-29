import com.microsoft.azure.kusto.ingest.BlobDescription;
import com.microsoft.azure.kusto.ingest.KustoIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestionProperties;
import com.microsoft.azure.kusto.ingest.ResourceManager;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
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
    KustoIngestionProperties props;

    @BeforeEach
    void setUp() {
        try {
            ingestClientMock = mock(KustoIngestClient.class);

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
    void ingestFromMultipleBlobsPaths() {
        try {
            doReturn(null).when(ingestClientMock).ingestFromMultipleBlobs(isA(List.class),isA(Boolean.class),isA(KustoIngestionProperties.class));

            List<String> blobPaths = new ArrayList<>();
            blobPaths.add("blobPath1");
            blobPaths.add("blobPath2");
            blobPaths.add("blobPath3");

            ingestClientMock.ingestFromMultipleBlobsPaths(blobPaths, true, props);

            verify(ingestClientMock).ingestFromMultipleBlobsPaths(blobPaths, true, props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromSingleBlob() {
        try {
            doReturn(null).when(ingestClientMock).ingestFromMultipleBlobs(isA(List.class),isA(Boolean.class),isA(KustoIngestionProperties.class));

            String blobPath = "blobPath";
            long size = 100;

            ingestClientMock.ingestFromSingleBlob(blobPath, true, props, size);

            verify(ingestClientMock).ingestFromSingleBlob(blobPath, true, props, size);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromMultipleBlobs() {
        List<BlobDescription> blobs = new ArrayList<>();

        int numberOfBlobs = 3;
        for(int i=0; i<numberOfBlobs; i++){
            blobs.add(new BlobDescription("blobPath"+i, 1000L));
        }

        try {
            ingestClientMock.ingestFromMultipleBlobs(blobs,true, props);

            verify(ingestClientMock).ingestFromMultipleBlobs(anyList(),anyBoolean(),any(KustoIngestionProperties.class));
            //verify(ingestClientMock,times(numberOfBlobs)).postMessageToQueue(anyString(),anyString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromSingleFile() {
        try {
            String testFilePath = Paths.get("src","test","resources","testdata.json").toString();

            when(ingestClientMock.uploadLocalFileToBlob(isA(String.class),isA(String.class),isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(ingestClientMock).postMessageToQueue(isA(String.class),isA(String.class));

            int numOfFiles = 3;
            for(int i=0; i<numOfFiles; i++){
                ingestClientMock.ingestFromSingleFile(testFilePath,props);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromSingleFile(testFilePath,props);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStream() {
        try {
            String testFilePath = Paths.get("src","test","resources","testdata.json").toString();
            InputStream stream = new FileInputStream(testFilePath);

            when(ingestClientMock.uploadFromStreamToBlob(isA(InputStream.class),isA(String.class),isA(String.class),isA(Boolean.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(ingestClientMock).postMessageToQueue(isA(String.class),isA(String.class));

            int numOfFiles = 3;
            for(int i=0; i<numOfFiles; i++){
                ingestClientMock.ingestFromStream(stream,props,false,false);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromStream(any(InputStream.class),any(KustoIngestionProperties.class),anyBoolean(),anyBoolean());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStreamThroughTempFile() {
        try {
            String testFilePath = Paths.get("src","test","resources","testdata.json").toString();
            InputStream stream = new FileInputStream(testFilePath);

            when(ingestClientMock.uploadLocalFileToBlob(isA(String.class),isA(String.class),isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(ingestClientMock).postMessageToQueue(isA(String.class),isA(String.class));

            int numOfFiles = 3;
            for(int i=0; i<numOfFiles; i++){
                ingestClientMock.ingestFromStream(stream,props,false,true);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromStream(any(InputStream.class),any(KustoIngestionProperties.class),anyBoolean(),anyBoolean());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}