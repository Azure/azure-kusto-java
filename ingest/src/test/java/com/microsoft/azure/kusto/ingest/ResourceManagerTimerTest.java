package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.ingest.Commands;
import com.microsoft.azure.kusto.ingest.ResourceManager;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionAuthTokenResult;
import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionResourcesResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceManagerTimerTest {

    @Test
    void TimerTest() throws DataClientException, DataServiceException, InterruptedException, KustoServiceQueryError, IOException {
        Client mockedClient = mock(Client.class);
        final List<Date> refreshTimestamps = new ArrayList<>();
        when(mockedClient.execute(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());
        when(mockedClient.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND)).then((Answer) invocationOnMock -> {
            refreshTimestamps.add((new Date()));
            if (refreshTimestamps.size() != 1) {
                throw new Exception();
            }

            return generateIngestionResourcesResult();
        });

        ResourceManager resourceManager = new ResourceManager(mockedClient, 1000L, 500L, null);
        Thread.sleep(100);
        assertEquals(1, refreshTimestamps.size());
        Thread.sleep(1100);
        assertEquals(2, refreshTimestamps.size());
        Thread.sleep(600);
        assertEquals(3, refreshTimestamps.size());
        resourceManager.close();
    }
}
