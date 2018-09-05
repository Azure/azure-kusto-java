package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class AzureStorageHelperTest {
    AzureStorageHelper azureStorageHelperMock = mock(AzureStorageHelper.class);
    CloudBlockBlob cloudBlockBlobMock = mock(CloudBlockBlob.class);

    @BeforeEach
    void setUp() {
        try {
            doNothing().when(cloudBlockBlobMock).uploadFromFile(isA(String.class));
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void postMessageToQueue() {
    }

    @Test
    void azureTableInsertEntity() {
    }

    @Test
    void uploadLocalFileToBlob() {
        try{
            GZIPOutputStream gzipOutputStreamMock = mock(GZIPOutputStream.class);
            doNothing().when(gzipOutputStreamMock).write(any(byte[].class),anyInt(),anyInt());

            String testFilePath = Paths.get("src","test","resources","testdata.json").toString();

            azureStorageHelperMock.uploadLocalFileToBlob(testFilePath,"blobName","https://ms.com/blob");

            verify(azureStorageHelperMock).uploadLocalFileToBlob(anyString(),anyString(),anyString());

        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] bs = new byte[12];
    }

    @Test
    void getBlobPathWithSas() {
    }
}