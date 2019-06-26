package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.TableReportIngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;

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
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.INGESTION_MAPPING_KIND.json);
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
    void IngestFromResultSet_DefaultTempFolder_Success() throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept calls to internal methods so it wouldn't be called
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        long numberOfChars = 1000;
        doReturn(numberOfChars).when(ingestClientSpy).writeResultSetToWriterAsCsv(any(), any(), anyBoolean());

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
    void IngestFromResultSet_SpecifyTempFolder_Success() throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        long numberOfChars = 1000;
        doReturn(numberOfChars).when(ingestClientSpy).writeResultSetToWriterAsCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        File tempPath = new File(
                Paths.get(System.getProperty("java.io.tmpdir"), String.valueOf(System.currentTimeMillis())).toString());
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
    void IngestFromResultSet_ResultSetToCsv_IngestionClientException()
            throws IngestionClientException, IngestionServiceException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        doThrow(new IngestionClientException("error in resultSetToCsv"))
                .when(ingestClientSpy)
                .writeResultSetToWriterAsCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        assertThrows(
                IngestionClientException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_FileIngest_IngestionClientException()
            throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionClientException ingestionClientException = new IngestionClientException(
                "Client exception in ingestFromFile");
        doThrow(ingestionClientException).when(ingestClientSpy).ingestFromFile(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(
                IngestionClientException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void IngestFromResultSet_FileIngest_IngestionServiceException()
            throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        // we need a spy to intercept the call to ingestFromFile so it wouldn't be called
        IngestClient ingestClientSpy = spy(ingestClient);

        IngestionServiceException ingestionServiceException = new IngestionServiceException(
                "Service exception in ingestFromFile");
        doThrow(ingestionServiceException).when(ingestClientSpy).ingestFromFile(any(), any());

        ResultSet resultSet = getSampleResultSet();
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

        assertThrows(
                IngestionServiceException.class,
                () -> ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties));
    }

    @Test
    void ResultSetToCsv_Success() throws SQLException, IngestionClientException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();
        StringWriter stringWriter = new StringWriter();
        long numberOfCharsActual = ingestClient.writeResultSetToWriterAsCsv(resultSet, stringWriter, false);

        final String expected = getSampleResultSetDump();

        assertEquals(expected, stringWriter.toString()); // check the string values
        assertEquals(expected.length(), numberOfCharsActual); // check the returned length
    }

    @Test
    void ResultSetToCsv_ClosedResultSet_Exception() throws SQLException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();
        resultSet.close();
        StringWriter stringWriter = new StringWriter();

        assertThrows(
                IngestionClientException.class,
                () -> ingestClient.writeResultSetToWriterAsCsv(resultSet, stringWriter, false));
    }

    @Test
    void ResultSetToCsv_Writer_Exception() throws SQLException, IOException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ResultSet resultSet = getSampleResultSet();

        Writer writer = mock(Writer.class);
        doThrow(new IOException("Some exception")).when(writer).write(anyString());

        assertThrows(
                IngestionClientException.class,
                () -> ingestClient.writeResultSetToWriterAsCsv(resultSet, writer, false));
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

    @Test
    void IngestFromBlob_IngestionReportMethodIsTable_RemovesSecrets() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud", 100);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
        ArgumentCaptor<TableServiceEntity> captur = ArgumentCaptor.forClass(TableServiceEntity.class);

        ingestClientImpl.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).azureTableInsertEntity(anyString(), captur.capture());
        assert (((IngestionStatus) captur.getValue()).getIngestionSourcePath()).equals("https://storage.table.core.windows.net/ingestionsstatus20190505");
    }
}
