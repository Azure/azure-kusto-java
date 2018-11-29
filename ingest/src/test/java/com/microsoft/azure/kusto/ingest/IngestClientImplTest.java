package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.TableReportIngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class IngestClientImplTest {

    private ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private IngestClientImpl ingestClientImplMock;
    private AzureStorageHelper azureStorageHelperMock;
    private IngestionProperties ingestionProperties;

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

            ingestionProperties = new IngestionProperties("dbName", "tableName");
            ingestionProperties.setJsonMappingName("mappingName");

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

            ingestClientImplMock.ingestFromBlob(blobSourceInfo, ingestionProperties);

            verify(ingestClientImplMock).ingestFromBlob(blobSourceInfo, ingestionProperties);

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
                ingestClientImplMock.ingestFromFile(fileSourceInfo, ingestionProperties);
            }

            verify(ingestClientImplMock, times(numOfFiles)).ingestFromFile(fileSourceInfo, ingestionProperties);

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
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
                ingestClientImplMock.ingestFromStream(streamSourceInfo, ingestionProperties);
            }
            verify(ingestClientImplMock, times(numOfFiles)).ingestFromStream(any(StreamSourceInfo.class), any(IngestionProperties.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromResultSet_Success() throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
        // we need a spy to intercept calls to internal methods so it wouldn't be called
        IngestClientImpl ingestClientSpy = spy(ingestClient);
        TableReportIngestionResult ingestionResultMock = mock(TableReportIngestionResult.class);

        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());
        doNothing().when(ingestClientSpy).resultSetToCsv(any(), any(), anyBoolean());

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mock(ResultSet.class));

        ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties);

        verify(ingestClientSpy).ingestFromResultSet(resultSetSourceInfo, ingestionProperties);
        verify(ingestClientSpy).ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class));
    }

    @Test
    void ingestFromResultSet_ResultSetToCsv_Error() throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
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
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
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
        IngestClient ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
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
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
        ResultSet resultSet = getSampleResultSet();
        StringWriter stringWriter = new StringWriter();

        ingestClient.resultSetToCsv(resultSet, stringWriter, false);

        final String expected = System.getProperty("line.separator").equals("\n") ?
                "\"1\",\"leo\"\n\"2\",\"yui\"\n" :
                "\"1\",\"leo\"\r\n\"2\",\"yui\"\r\n";

        assertEquals(expected, stringWriter.toString());
    }

    @Test
    void resultSetToCsv_ResultSetClosed_Exception() throws IngestionClientException, IngestionServiceException, SQLException {
        IngestClientImpl ingestClient = new IngestClientImpl(resourceManagerMock, azureStorageHelperMock);
        ResultSet resultSet = getSampleResultSet();
        resultSet.close();
        StringWriter stringWriter = new StringWriter();

        assertThrows(IngestionClientException.class,
                () -> ingestClient.resultSetToCsv(resultSet, stringWriter, false));
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
}
