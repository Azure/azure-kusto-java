// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestionProperties.DataFormat;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.result.ValidationPolicy;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QueuedIngestClientTest {
    private static final ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static final AzureStorageClient azureStorageClientMock = mock(AzureStorageClient.class);
    public static final String ACCOUNT_NAME = "someaccount";
    private static QueuedIngestClient queuedIngestClient;
    private static IngestionProperties ingestionProperties;
    private static String testFilePath;

    @BeforeAll
    static void setUp() throws Exception {
        testFilePath = Path.of("src", "test", "resources", "testdata.csv").toString();
        when(resourceManagerMock.getShuffledContainers())
                .thenReturn(Collections.singletonList(TestUtils.containerWithSasFromAccountNameAndContainerName(ACCOUNT_NAME, "someStorage")));
        when(resourceManagerMock.getShuffledQueues())
                .thenReturn(Collections.singletonList(TestUtils.queueWithSasFromAccountNameAndQueueName(ACCOUNT_NAME, "someQueue")));

        when(resourceManagerMock.getStatusTable())
                .thenReturn(TestUtils.tableWithSasFromTableName("http://statusTable.com"));

        when(resourceManagerMock.getIdentityToken()).thenReturn("identityToken");

        when(azureStorageClientMock.azureTableInsertEntity(any(), any(TableEntity.class)))
                .thenReturn(Mono.empty());

        when(azureStorageClientMock.postMessageToQueue(any(), anyString())).thenReturn(Mono.empty());
        when(azureStorageClientMock.uploadStreamToBlob(any(), any(), any(), anyBoolean())).thenReturn(Mono.empty());
    }

    @BeforeEach
    void setUpEach() throws IngestionServiceException {
        doReturn(Collections.singletonList(TestUtils.containerWithSasFromContainerName("storage")),
                Collections.singletonList(TestUtils.containerWithSasFromContainerName("storage2"))).when(resourceManagerMock)
                        .getShuffledContainers();

        queuedIngestClient = new QueuedIngestClientImpl(resourceManagerMock, azureStorageClientMock);
        ingestionProperties = new IngestionProperties("dbName", "tableName");
        ingestionProperties.setIngestionMapping("mappingName", IngestionMapping.IngestionMappingKind.CSV);
        ingestionProperties.setDataFormat(DataFormat.CSV);
    }

    @AfterEach
    void tareEach() throws IOException {
        queuedIngestClient.close();
    }

    @Test
    void ingestFromBlob_IngestionReportMethodIsNotTable_EmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://blobPath.blob.core.windows.net/container/blob");
        IngestionResult result = queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        assertEquals(OperationStatus.Queued, result.getIngestionStatusCollectionAsync().block().get(0).status);
    }

    @Test
    void ingestFromBlob_IngestionReportMethodIsTable_NotEmptyIngestionStatus() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://blobPath.blob.core.windows.net/container/blob");
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);

        IngestionResult result = queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
    }

    @Test
    void ingestFromBlob_NullIngestionProperties_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("https://blobPath.blob.core.windows.net/container/blob");
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromBlob(blobSourceInfo, null));
    }

    @Test
    void ingestFromBlob_NullBlobSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromBlob(null, ingestionProperties));
    }

    @Test
    void ingestFromBlob_IngestionReportMethodIsTable_RemovesSecrets() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                "https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud");
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ArgumentCaptor<TableEntity> captor = ArgumentCaptor.forClass(TableEntity.class);

        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).azureTableInsertEntity(any(), captor.capture());
        assert (IngestionStatus.fromEntity(captor.getValue()).getIngestionSourcePath())
                .equals("https://storage.table.core.windows.net/ingestionsstatus20190505");
    }

    @Test
    void ingestFromBlob_IngestionIgnoreFirstRecord_SetsProperty() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                "https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud");
        ingestionProperties.setIgnoreFirstRecord(true);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).postMessageToQueue(any(), captor.capture());
        assertTrue((captor.getValue()).contains("\"ignoreFirstRecord\":\"true\""));

        ingestionProperties.setIgnoreFirstRecord(false);
        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeast(1)).postMessageToQueue(any(), captor.capture());
        assertTrue((captor.getValue()).contains("\"ignoreFirstRecord\":\"false\""));
    }

    @Test
    void ingestFromBlob_ValidationPolicy_SetsProperly() throws Exception {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                "https://storage.table.core.windows.net/ingestionsstatus20190505?sv=2018-03-28&tn=ingestionsstatus20190505&sig=anAusomeSecret%2FK024xNydFzT%2B2cCE%2BA2S8Y6U%3D&st=2019-05-05T09%3A00%3A31Z&se=2019-05-09T10%3A00%3A31Z&sp=raud");
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);

        verify(azureStorageClientMock, atLeast(1)).postMessageToQueue(any(), captor.capture());
        assertFalse(captor.getValue().toLowerCase().contains("validationpolicy"));

        ingestionProperties.setValidationPolicy(new ValidationPolicy());
        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeast(1)).postMessageToQueue(any(), captor.capture());
        assertTrue(
                captor.getValue()
                        .contains(
                                "\"validationPolicy\":{\"validationOptions\":\"DoNotValidate\",\"validationPolicyType\":\"BestEffort\"}"));

        ingestionProperties.setValidationPolicy(
                new ValidationPolicy(ValidationPolicy.ValidationOptions.VALIDATE_CSV_INPUT_COLUMN_LEVEL_ONLY, ValidationPolicy.ValidationImplications.FAIL));
        queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        verify(azureStorageClientMock, atLeast(1)).postMessageToQueue(any(), captor.capture());
        assertTrue(
                captor.getValue()
                        .contains(
                                "\"validationPolicy\":{\"validationOptions\":\"ValidateCsvInputColumnLevelOnly\",\"validationPolicyType\":\"Fail\"}"));
    }

    @Test
    void ingestFromFile_NullIngestionProperties_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path");
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromFile(fileSourceInfo, null));
    }

    @Test
    void ingestFromFile_NullFileSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromFile(null, ingestionProperties));
    }

    @Test
    void ingestFromFileAsync_FileDoesNotExist_IngestionClientException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path");
        Mono<IngestionResult> result = queuedIngestClient.ingestFromFileAsync(fileSourceInfo, ingestionProperties);
        StepVerifier.create(result)
                .expectError(IngestionClientException.class)
                .verify();
    }

    @Test
    void ingestFromStream_UploadStreamToBlobIsCalled() throws Exception {
        InputStream stream = Files.newInputStream(Path.of(testFilePath));
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
        try {
            queuedIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (Exception e) {
            // Ignore the exception, we just want to verify the uploadStreamToBlob method is called
            assertTrue(e.getMessage().contains("Empty"));
        }
        verify(azureStorageClientMock, atLeastOnce())
                .uploadStreamToBlob(any(InputStream.class), anyString(), any(), anyBoolean());
    }

    @Test
    void ingestFromStream_NullIngestionProperties_IllegalArgumentException() {
        StreamSourceInfo streamSourceInfo = mock(StreamSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromStream(streamSourceInfo, null));
    }

    @Test
    void ingestFromStream_NullStreamSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromStream(null, ingestionProperties));
    }

    @Test
    void ingestFromResultSet_NullIngestionProperties_IllegalArgumentException() {
        ResultSetSourceInfo resultSetSourceInfo = mock(ResultSetSourceInfo.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromResultSet(resultSetSourceInfo, null));
    }

    @Test
    void ingestFromResultSet_NullResultSetSourceInfo_IllegalArgumentException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> queuedIngestClient.ingestFromResultSet(null, ingestionProperties));
    }

    @Test
    void ingestFromResultSetAsync_StreamIngest_IngestionClientException() throws Exception {
        try (IngestClient ingestClient = new QueuedIngestClientImpl(resourceManagerMock, azureStorageClientMock)) {
            // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
            IngestClient ingestClientSpy = spy(ingestClient);

            IngestionClientException ingestionClientException = new IngestionClientException(
                    "Client exception in ingestFromFile");
            doReturn(Mono.error(ingestionClientException)).when(ingestClientSpy).ingestFromStreamAsync(any(), any());

            ResultSet resultSet = getSampleResultSet();
            ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

            StepVerifier.create(ingestClientSpy.ingestFromResultSetAsync(resultSetSourceInfo, ingestionProperties))
                    .expectError(IngestionClientException.class)
                    .verify();
        }
    }

    @Test
    void ingestFromResultSetAsync_StreamIngest_IngestionServiceException() throws Exception {
        try (IngestClient ingestClient = new QueuedIngestClientImpl(resourceManagerMock, azureStorageClientMock)) {
            // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
            IngestClient ingestClientSpy = spy(ingestClient);

            IngestionServiceException ingestionServiceException = new IngestionServiceException(
                    "Service exception in ingestFromFile");
            doReturn(Mono.error(ingestionServiceException)).when(ingestClientSpy).ingestFromStreamAsync(any(), any());

            ResultSet resultSet = getSampleResultSet();
            ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

            StepVerifier.create(ingestClientSpy.ingestFromResultSetAsync(resultSetSourceInfo, ingestionProperties))
                    .expectError(IngestionServiceException.class)
                    .verify();
        }
    }

    @Test
    void ingestFromResultSet_StreamIngest_VerifyStreamContent() throws Exception {
        try (IngestClient ingestClient = new QueuedIngestClientImpl(resourceManagerMock, azureStorageClientMock)) {
            // we need a spy to intercept the call to ingestFromStream so it wouldn't be called
            IngestClient ingestClientSpy = spy(ingestClient);

            doReturn(Mono.empty()).when(ingestClientSpy).ingestFromStreamAsync(any(), any());

            ResultSet resultSet = getSampleResultSet();
            ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(resultSet);

            ingestClientSpy.ingestFromResultSet(resultSetSourceInfo, ingestionProperties);

            ArgumentCaptor<StreamSourceInfo> argumentCaptor = ArgumentCaptor.forClass(StreamSourceInfo.class);

            verify(ingestClientSpy, atLeastOnce()).ingestFromStreamAsync(argumentCaptor.capture(), any());
            InputStream ingestFromStreamReceivedStream = argumentCaptor.getValue().getStream();

            int len = ingestFromStreamReceivedStream.available();
            byte[] streamContent = new byte[len];
            ingestFromStreamReceivedStream.read(streamContent, 0, len);
            String stringContent = new String(streamContent);
            assertEquals(stringContent, getSampleResultSetDump());
        }
    }

    private static Stream<Arguments> provideParametersForAutoCorrectEndpoint() {
        return Stream.of(
                Arguments.of(true, "https://testendpoint.dev.kusto.windows.net", "https://ingest-testendpoint.dev.kusto.windows.net"),
                Arguments.of(false, "https://testendpoint.dev.kusto.windows.net", "https://testendpoint.dev.kusto.windows.net"));
    }

    @ParameterizedTest
    @MethodSource("provideParametersForAutoCorrectEndpoint")
    void autoCorrectEndpoint(boolean shouldAutoCorrectEndpoint, String inputUrl, String expectedUrl) throws URISyntaxException {
        /*
         * Would be better to pass resourceManagerMock and azureStorageClientMock so we can ensure no external calls or other heavy actions are taken in this
         * unit test, but the QueuedIngestClientImpl constructor either takes the component parts (CSB + properties + autoCorrectEndpoint boolean) and then uses
         * these to set the 2 main members it requires (ResourceManager + AzureStorageClient), or accepts these 2 main members directly without the component
         * parts because they aren't needed in that case. We close the client in the @AfterEach method, so this may be hardly heavier to execute than if it were
         * a pure unit test.
         */
        queuedIngestClient = IngestClientFactory.createClient(ConnectionStringBuilder.createWithUserPrompt(inputUrl), null, shouldAutoCorrectEndpoint);
        assertNotNull(queuedIngestClient);
        assertEquals(expectedUrl, ((QueuedIngestClientImpl) queuedIngestClient).connectionDataSource);
    }

    @Test
    void generateName() {
        QueuedIngestClientImpl ingestClient = new QueuedIngestClientImpl(resourceManagerMock, azureStorageClientMock);
        class Holder {
            private String name;
        }
        final Holder holder = new Holder();
        holder.name = "fileName";
        BiFunction<DataFormat, CompressionType, String> genName = (DataFormat format, CompressionType compression) -> {
            boolean shouldCompress = IngestClientBase.shouldCompress(compression, format);
            return ingestClient.genBlobName(
                    holder.name,
                    "db1",
                    "t1",
                    format.getKustoValue(),
                    shouldCompress ? CompressionType.gz : compression);
        };
        String csvNoCompression = genName.apply(DataFormat.CSV, null);
        assert (csvNoCompression.endsWith(".csv.gz"));

        String csvCompression = genName.apply(DataFormat.CSV, CompressionType.zip);
        assert (csvCompression.endsWith(".csv.zip"));

        String parquet = genName.apply(DataFormat.PARQUET, null);
        assert (parquet.endsWith(".parquet"));

        String avroLocalFileName = "avi.avro";
        String avroLocalCompressFileName = "avi.avro.gz";
        CompressionType compressionTypeRes = IngestionUtils.getCompression(avroLocalFileName);
        CompressionType compressionTypeRes2 = IngestionUtils.getCompression(avroLocalCompressFileName);
        holder.name = avroLocalFileName;
        String avroName = genName.apply(DataFormat.AVRO, compressionTypeRes);
        assert (avroName.endsWith(".avro"));

        holder.name = avroLocalCompressFileName;
        String avroNameCompression = genName.apply(DataFormat.AVRO, compressionTypeRes2);
        assert (avroNameCompression.endsWith(".avro.gz"));
        ingestClient.close();
    }

    private ResultSet getSampleResultSet() throws SQLException {
        // create a database connection
        Connection connection = DriverManager.getConnection("jdbc:sqlite:");

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(5); // set timeout to 5 sec.

        statement.executeUpdate("drop table if exists person");
        statement.executeUpdate("create table person (id integer, name string)");
        statement.executeUpdate("insert into person values(1, 'leo')");
        statement.executeUpdate("insert into person values(2, 'yui')");

        return statement.executeQuery("select * from person");
    }

    private String getSampleResultSetDump() {
        return System.lineSeparator().equals("\n") ? "1,leo\n2,yui\n" : "1,leo\r\n2,yui\r\n";
    }
}
