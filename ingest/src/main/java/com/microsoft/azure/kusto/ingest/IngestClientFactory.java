package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) {
        Client client = ClientFactory.createClient(csb);
        ResourceManager resourceManager = new ResourceManager(client);
        AzureStorageHelper azureStorageHelper = new AzureStorageHelper();

        return new IngestClientImpl(resourceManager, azureStorageHelper);
    }
}
