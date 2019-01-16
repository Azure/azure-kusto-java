package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        Client client = ClientFactory.createClient(csb);
        ResourceManager resourceManager = new ResourceManager(client);
        AzureStorageHelper azureStorageHelper = new AzureStorageHelper();

        return new IngestClientImpl(resourceManager, azureStorageHelper);
    }
}
