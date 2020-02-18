package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.source.*;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;
import java.util.function.BiFunction;

import com.microsoft.azure.kusto.ingest.IngestionProperties.DATA_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class IngestClientImplTest {

    private static ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    private static IngestClientImpl ingestClientImpl;
    private static IngestionProperties ingestionProperties;
    private static String testFilePath;

    @BeforeAll
    static void setUp() throws Exception {
        testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
        ingestClientImpl = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);

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

        when(azureStorageClientMock.uploadStreamToBlob(any(InputStream.class), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

        when(azureStorageClientMock.getBlobPathWithSas(any(CloudBlockBlob.class)))
                .thenReturn("https://ms.com/storageUri");

        when(azureStorageClientMock.getBlobSize(anyString())).thenReturn(100L);

        when(azureStorageClientMock.uploadLocalFileToBlob(anyString(), anyString(), anyString(), anyBoolean()))
                .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

        doNothing().when(azureStorageClientMock)
                .azureTableInsertEntity(anyString(), any(TableServiceEntity.class));

        doNothing().when(azureStorageClientMock)
                .postMessageToQueue(anyString(), anyString());
    }

    @BeforeEach
    void setUpEach() {
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.IngestionMappingKind.Json);
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsNotTable_EmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assert result.getIngestionStatusesLength() == 0;
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_NotEmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);

        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assert result.getIngestionStatusesLength() != 0;
    }

    @Test
    void IngestFromBlob_NullIngestionProperties_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromBlob(blobSourceInfo, null));
    }

    @Test
    void IngestFromBlob_NullBlobSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromBlob(null, ingestionProperties));
    }

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_RemovesSecrets() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
        ArgumentCaptor<TableServiceEntity> captur = ArgumentCaptor.forClass(TableServiceEntity.class);

        ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).azureTableInsertEntity(anyString(), captur.capture());
        assert (((IngestionStatus) captur.getValue()).getIngestionSourcePath()).equals("https://storage.table.core.windows.net/ingestionsstatus20190505");
    }

    @Test
    void IngestFromFile_GetBlobPathWithSasIsCalled() throws Exception {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 100);
        ingestClientImpl.ingestFromFile(fileSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeastOnce()).getBlobPathWithSas(any(CloudBlockBlob.class));
    }

    @Test
    void IngestFromFile_NullIngestionProperties_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, null));
    }

    @Test
    void IngestFromFile_NullFileSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(null, ingestionProperties));
    }

    @Test
    void IngestFromFile_FileDoesNotExist_IngestionClientException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);
        assertThrows(
                IngestionClientException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromStream_GetBlobPathWithSasIsCalled() throws Exception {
        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
        ingestClientImpl.ingestFromStream(streamSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeastOnce()).getBlobPathWithSas(any(CloudBlockBlob.class));
    }

    @Test
    void IngestFromStream_UploadStreamToBlobIsCalled() throws Exception {
        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
        ingestClientImpl.ingestFromStream(streamSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeastOnce())
                .uploadStreamToBlob(any(InputStream.class), anyString(), anyString(), anyBoolean());
    }

    @Test
    void IngestFromStream_NullIngestionProperties_IllegalArgumentException() {
        StreamSourceInfo streamSourceInfo = mock(StreamSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromStream(streamSourceInfo, null));
    }

    @Test
    void IngestFromStream_NullStreamSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromStream(null, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_NullIngestionProperties_IllegalArgumentException() {
        ResultSetSourceInfo resultSetSourceInfo = mock(ResultSetSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromResultSet(resultSetSourceInfo, null));
    }

    @Test
    void IngestFromResultSet_NullResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromResultSet(null, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_StreamIngest_IngestionClientException() throws Exception {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionClientException ingestionClientException = new IngestionClientException(
                "Client exception in ingestFromFile");
        doThrow(ingestionClientException).when(ingestClientSpy).ingestFromStream(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(
                IngestionClientException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_StreamIngest_IngestionServiceException() throws Exception {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionServiceException ingestionServiceException = new IngestionServiceException(
                "Service exception in ingestFromFile");
        doThrow(ingestionServiceException).when(ingestClientSpy).ingestFromStream(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(
                IngestionServiceException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_StreamIngest_VerifyStreamContent() throws Exception {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        doReturn(null).when(ingestClientSpy).ingestFromStream(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties);

        ArgumentCaptor<StreamSourceInfo> argumentCaptor = ArgumentCaptor.forClass(StreamSourceInfo.class);

        verify(ingestClientSpy, atLeastOnce()).ingestFromStream(argumentCaptor.capture(), any());
        InputStream ingestFromStreamReceivedStream = argumentCaptor.getValue().getStream();

        int len = ingestFromStreamReceivedStream.available();
        byte[] streamContent = new byte[len];
        ingestFromStreamReceivedStream.read(streamContent, 0, len);
        String stringContent = new String(streamContent);
        assertEquals(stringContent, getSampleResultSetDump());
    }
    @Test

    void generateName() {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        class Holder {
            private String name;
        }
        final Holder holder = new Holder();
        holder.name = "fileName";
        BiFunction<IngestionProperties.DATA_FORMAT, CompressionType, String> genName =
            (IngestionProperties.DATA_FORMAT format, CompressionType compression) -> {
            boolean shouldCompress = AzureStorageClient.shouldCompress(compression, format.name());
            return ingestClient.genBlobName(
                    holder.name,
                    "db1",
                    "t1",
                    format.name(),
                    shouldCompress ? CompressionType.gz : compression);
        };
        String csvNoCompression = genName.apply(DATA_FORMAT.csv, null);
        assert(csvNoCompression.endsWith("fileName.Csv.gz"));

        String csvCompression = genName.apply(DATA_FORMAT.csv, CompressionType.zip);
        assert(csvCompression.endsWith("fileName.Csv.zip"));

        String parquet = genName.apply(DATA_FORMAT.parquet, null);
        assert(parquet.endsWith("fileName.parquet"));

        String avroLocalFileName = "avi.avro";
        String avroLocalCompressFileName = "avi.avro.gz";
        CompressionType compressionTypeRes = AzureStorageClient.getCompression(avroLocalFileName);
        CompressionType compressionTypeRes2 = AzureStorageClient.getCompression(avroLocalCompressFileName);
        holder.name = avroLocalFileName;
        String avroName = genName.apply(DATA_FORMAT.avro, compressionTypeRes);
        assert(avroName.endsWith("avi.avro.avro.gz"));

        holder.name = avroLocalCompressFileName;
        String avroNameCompression = genName.apply(DATA_FORMAT.avro, compressionTypeRes2);
        assert(avroNameCompression.endsWith("avi.avro.gz.avro.gz"));
    }

    private ResultSet getSampleResultSet() throws SQLException {
        // create a database connection
        Connection connection = DriverManager.getConnection("jdbc:sqlite:");

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(5);  // set timeout to 5 sec.

        statement.executeUpdate("drop table if exists person");
        statement.executeUpdate("create table person (id integer, name string)");
        statement.executeUpdate("insert into person values(1, 'leo')");
        statement.executeUpdate("insert into person values(2, 'yui')");

        return statement.executeQuery("select * from person");
    }

    private String getSampleResultSetDump() {
        return System.getProperty("line.separator").equals("\n") ?
                "1,leo\n2,yui\n" :
                "1,leo\r\n2,yui\r\n";
    }


}
