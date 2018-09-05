package com.microsoft.azure.kusto.ingest.result;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

public class TableReportKustoIngestionResult implements KustoIngestionResult {

    private List<IngestionStatusInTableDescription> descriptors;

    public TableReportKustoIngestionResult(List<IngestionStatusInTableDescription> descriptors) {
        this.descriptors = descriptors;
    }

    @Override
    public List<IngestionStatus> GetIngestionStatusCollection() throws StorageException, URISyntaxException {
        List<IngestionStatus> results = new LinkedList<>();
        for (IngestionStatusInTableDescription descriptor : descriptors) {
            CloudTable table = new CloudTable(new URI(descriptor.TableConnectionString));
            TableOperation operation = TableOperation.retrieve(descriptor.PartitionKey, descriptor.RowKey,
                    IngestionStatus.class);
            results.add(table.execute(operation).getResultAsType());
        }

        return results;
    }

}
