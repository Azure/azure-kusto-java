package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.ingest.exceptions.KustoClientException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

public class IngestFromMultipleBlobsCallable implements Callable<Object>  {

    private List<String> blobPaths;
    private Boolean deleteSourceOnSuccess;
    private KustoIngestionProperties ingestionProperties;
    private final String ingestionQueueUri;


    public IngestFromMultipleBlobsCallable(List<String> blobPaths, Boolean deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties, final String ingestionQueueUri)
    {
        this.blobPaths = blobPaths;
        this.deleteSourceOnSuccess = deleteSourceOnSuccess;
        this.ingestionProperties = ingestionProperties;
        this.ingestionQueueUri = ingestionQueueUri;
    }

    @Override
    public Object call() throws Exception {
        if(blobPaths == null || blobPaths.size() == 0)
        {
            throw new KustoClientException("blobs must have at least 1 path");
        }

        List<KustoClientException> ingestionErrors = new LinkedList<KustoClientException>();

        for (String blobPath : blobPaths)
        {
            try {
                // Create the ingestion message
                IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobPath, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
                ingestionBlobInfo.rawDataSize = estimateBlobRawSize(blobPath);
                ingestionBlobInfo.retainBlobOnSuccess = !deleteSourceOnSuccess;
                ingestionBlobInfo.reportLevel = ingestionProperties.getReportLevel();
                ingestionBlobInfo.reportMethod = ingestionProperties.getReportMethod();
                ingestionBlobInfo.flushImmediately = ingestionProperties.getFlushImmediately();
                ingestionBlobInfo.additionalProperties = ingestionProperties.getAdditionalProperties();

                ObjectMapper objectMapper = new ObjectMapper();
                String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

                AzureStorageHelper.postMessageToQueue(ingestionQueueUri, serializedIngestionBlobInfo);
            }
            catch (Exception ex)
            {
                ingestionErrors.add(new KustoClientException(blobPath, "fail to post message to queue", ex));
            }
        }

        if(ingestionErrors.size() > 0)
        {
            throw new KustoClientAggregateException(ingestionErrors);
        }
        return null;
    }



    private Long estimateBlobRawSize(String blobPath){

        try {
            CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
            blockBlob.downloadAttributes();
            long length = blockBlob.getProperties().getLength();

            if (length == 0)
            {
                return null;
            }
            return length;

        } catch (Exception e) {
            return null;
        }
    }
}
