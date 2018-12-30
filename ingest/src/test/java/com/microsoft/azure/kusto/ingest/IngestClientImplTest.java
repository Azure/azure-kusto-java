package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class IngestClientImplTest {

    private static ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static AzureStorageHelper azureStorageHelperMock = mock(AzureStorageHelper.class);
    private static IngestClientImpl ingestClientImpl;
    private static IngestionProperties props;

    @BeforeAll
    static void setUp() throws Exception {
        ingestClientImpl = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);

        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE))
                .thenReturn("queue1")
                .thenReturn("queue2");

        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE))
                .thenReturn("storage1")
                .thenReturn("storage2");

        when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE))
                .thenReturn("http://statusTable.com");

        when(resourceManagerMock.getIdentityToken())
                .thenReturn("identityToken");

        when(azureStorageHelperMock.uploadStreamToBlob(any(InputStream.class), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

        when(azureStorageHelperMock.getBlobPathWithSas(any(CloudBlockBlob.class)))
                .thenReturn("https://ms.com/storageUri");

        when(azureStorageHelperMock.getBlobSize(anyString())).thenReturn(100L);

        when(azureStorageHelperMock.uploadLocalFileToBlob(anyString(), anyString(), anyString()))
                .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

        doNothing().when(azureStorageHelperMock)
                .azureTableInsertEntity(anyString(), any(TableServiceEntity.class));

        doNothing().when(azureStorageHelperMock)
                .postMessageToQueue(anyString(), anyString());
    }

    @BeforeEach
    void setUpEach() {
        props = new IngestionProperties("dbName", "tableName");
        props.setJsonMappingName("mappingName");
    }

    @Test
    void ingestFromBlobCheckIngestionStatusEmpty() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, props);
        assert result.getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromBlobCheckIngestionStatusNotEmptyInTableReportMethod() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        props.setReportMethod(IngestionProperties.IngestionReportMethod.Table);

        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, props);
        assert result.getIngestionStatusesLength() != 0;
    }

    @Test
    void ingestFromBlobThrowExceptionWhenArgumentIsNull() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromBlob(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromBlob(blobSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromBlob(null, props));
    }

    @Test
    void ingestFromFileCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 100);
        IngestionResult result = ingestClientImpl.ingestFromFile(fileSourceInfo, props);

        assert result.getIngestionStatusesLength() == 0;

        verify(azureStorageHelperMock).getBlobPathWithSas(any(CloudBlockBlob.class));
    }

    @Test
    void ingestFromFileThrowExceptionWhenArgumentIsNull() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(null, props));
    }

    @Test
    void ingestFromFileThrowExceptionWhenFileDoesNotExist() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, props));
    }

    @Test
    void ingestFromStreamCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
        IngestionResult result = ingestClientImpl.ingestFromStream(streamSourceInfo, props);
        assert result.getIngestionStatusesLength() == 0;

    }

    @Test
    void ingestFromBlobAsync() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath", 100);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromBlobAsync(blobSourceInfo, props);

        assertNotNull(cf);
        assert cf.get().getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromFileAsync() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromFileAsync(fileSourceInfo, props);

        assertNotNull(cf);
        assert cf.get().getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromStreamAsync() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromStreamAsync(streamSourceInfo, props);

        assertNotNull(cf);
        assert cf.get().getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromStreamThrowExceptionWhenArgumentIsNull() {
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(null);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromStream(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromStream(streamSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromStream(null, props));
    }
}
