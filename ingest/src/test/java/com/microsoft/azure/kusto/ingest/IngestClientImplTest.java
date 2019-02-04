package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.TableReportIngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class IngestClientImplTest {

    private static ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    private static IngestClientImpl ingestClientImpl;
    private static IngestionProperties ingestionProperties;

    @BeforeAll
    static void setUp() throws Exception {
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

        when(azureStorageClientMock.uploadLocalFileToBlob(anyString(), anyString(), anyString()))
                .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

        doNothing().when(azureStorageClientMock)
                .azureTableInsertEntity(anyString(), any(TableServiceEntity.class));

        doNothing().when(azureStorageClientMock)
                .postMessageToQueue(anyString(), anyString());
    }

    @BeforeEach
    void setUpEach() {
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setJsonMappingName("mappingName");
    }

    @Test
    void ingestFromBlobCheckIngestionStatusEmpty() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assert result.getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromBlobCheckIngestionStatusNotEmptyInTableReportMethod() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath.com", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);

        IngestionResult result = ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);
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
                () -> ingestClientImpl.ingestFromBlob(null, ingestionProperties));
    }

    @Test
    void ingestFromFileCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 100);
        IngestionResult result = ingestClientImpl.ingestFromFile(fileSourceInfo, ingestionProperties);

        assert result.getIngestionStatusesLength() == 0;

        verify(azureStorageClientMock).getBlobPathWithSas(any(CloudBlockBlob.class));
    }

    @Test
    void ingestFromFileThrowExceptionWhenArgumentIsNull() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClientImpl.ingestFromFile(null, ingestionProperties));
    }

    @Test
    void ingestFromFileThrowExceptionWhenFileDoesNotExist() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IngestionClientException.class,
                () -> ingestClientImpl.ingestFromFile(fileSourceInfo, ingestionProperties));
    }

    @Test
    void ingestFromStreamCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
        IngestionResult result = ingestClientImpl.ingestFromStream(streamSourceInfo, ingestionProperties);
        assert result.getIngestionStatusesLength() == 0;

    }

    @Test
    void ingestFromBlobAsyncCheckIngestionStatusEmpty() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("http://blobPath", 100);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromBlobAsync(blobSourceInfo, ingestionProperties);

        assertNotNull(cf);
        assert cf.get().getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromFileAsyncCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromFileAsync(fileSourceInfo, ingestionProperties);

        assertNotNull(cf);
        assert cf.get().getIngestionStatusesLength() == 0;
    }

    @Test
    void ingestFromStreamAsyncCheckIngestionStatusEmpty() throws Exception {
        String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();

        InputStream stream = new FileInputStream(testFilePath);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
        CompletableFuture<IngestionResult> cf = ingestClientImpl.ingestFromStreamAsync(streamSourceInfo, ingestionProperties);

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
                () -> ingestClientImpl.ingestFromStream(null, ingestionProperties));
    }

    @Test
    void ingestFromResultSet_DefaultTempFolder_Success() throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept calls to internal methods so it wouldn't be called
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        long numberOfChars = 1000;
        doReturn(numberOfChars).when(ingestClientSpy).resultSetToCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties);
        verify(ingestClientSpy).ingestFromResultSet(resultSetSourceInfo, ingestionProperties);

        // captor to allow us to inspect the internal call values
        ArgumentCaptor<FileSourceInfo> fileSourceInfoCaptor = ArgumentCaptor.forClass(FileSourceInfo.class);
        verify(ingestClientSpy).ingestFromFile(fileSourceInfoCaptor.capture(), any(IngestionProperties.class));
        FileSourceInfo fileSourceInfoActual = fileSourceInfoCaptor.getValue();
        assertEquals(numberOfChars * 2, fileSourceInfoActual.getRawSizeInBytes());
    }

    @Test
    void ingestFromResultSet_SpecifyTempFolder_Success() throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        long numberOfChars = 1000;
        doReturn(numberOfChars).when(ingestClientSpy).resultSetToCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        File tempPath = new File(Paths.get(System.getProperty("java.io.tmpdir"), String.valueOf(System.currentTimeMillis())).toString());
        //noinspection ResultOfMethodCallIgnored
        tempPath.mkdirs();

        String tempFolderPath = tempPath.toString();

        ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties, tempFolderPath);
        verify(ingestClientSpy).ingestFromResultSet(resultSetSourceInfo, ingestionProperties, tempFolderPath);

        // captor to allow us to inspect the internal call values
        ArgumentCaptor<FileSourceInfo> fileSourceInfoCaptor = ArgumentCaptor.forClass(FileSourceInfo.class);
        verify(ingestClientSpy).ingestFromFile(fileSourceInfoCaptor.capture(), any(IngestionProperties.class));
        FileSourceInfo fileSourceInfoActual = fileSourceInfoCaptor.getValue();
        assertEquals(numberOfChars * 2, fileSourceInfoActual.getRawSizeInBytes());
        // make sure the temp file was written to the folder we specified
        Assertions.assertTrue(fileSourceInfoActual.getFilePath().startsWith(tempFolderPath));
    }

    @Test
    void ingestFromResultSet_ResultSetToCsv_Error() throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        doThrow(new IngestionClientException("error in resultSetToCsv"))
                .when(ingestClientSpy)
                .resultSetToCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        assertThrows(IngestionClientException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void ingestFromResultSet_FileIngest_IngestionClientException() throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionClientException ingestionClientException = new IngestionClientException("Client exception in ingestFromFile");
        doThrow(ingestionClientException).when(ingestClientSpy).ingestFromFile(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(IngestionClientException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void ingestFromResultSet_FileIngest_IngestionServiceException() throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionServiceException ingestionServiceException = new IngestionServiceException("Service exception in ingestFromFile");
        doThrow(ingestionServiceException).when(ingestClientSpy).ingestFromFile(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(IngestionServiceException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void resultSetToCsv_Success() throws SQLException, IngestionClientException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();
        StringWriter stringWriter = new StringWriter();
        long numberOfCharsActual = ingestClient.resultSetToCsv(resultSet, stringWriter, false);

        final String expected = getSampleResultSetDump();

        assertEquals(expected, stringWriter.toString()); // check the string values
        assertEquals(expected.length(), numberOfCharsActual); // check the returned length
    }

    @Test
    void resultSetToCsv_ClosedResultSet_Exception() throws SQLException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();
        resultSet.close();
        StringWriter stringWriter = new StringWriter();

        assertThrows(IngestionClientException.class,
                () -> ingestClient.resultSetToCsv(resultSet, stringWriter, false));
    }

    @Test
    void resultSetToCsv_Writer_Exception() throws SQLException, IOException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();

        Writer writer = mock(Writer.class);
        doThrow(new IOException("Some exception")).when(writer).write(anyString());

        assertThrows(IngestionClientException.class,
                () -> ingestClient.resultSetToCsv(resultSet, writer, false));
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
                "\"1\",\"leo\"\n\"2\",\"yui\"\n" :
                "\"1\",\"leo\"\r\n\"2\",\"yui\"\r\n";
    }
}
