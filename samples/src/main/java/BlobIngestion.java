import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class BlobIngestion {
    public static void main(String[] args) {
        /* replace following variable values with your own credential information */

        // blob info
        String accountName = "<accountName>";
        String accountKey = "<accountKey>";
        String container = "<container>";
        String blobFileName = "example.blob.file.json";

        // Kusto credential
        String appId = "<appId>";
        String appKey = "<appKey>";
        String appTenant = "<appTenant>";

        // Kusto cluster/db/table information
        String kustoCluster = "<kustoCluster>";
        String kustoDBName = "<kustoDBName>";
        String kustoTableName = "<kustoTableName>";
        String jsonMapping = "<jsonMapping>";

        try {
            // build up Kusto IngestClient with Kusto credential
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    "https://ingest-" + kustoCluster + ".kusto.windows.net",
                    appId, appKey, appTenant);
            IngestClient client = IngestClientFactory.createClient(csb);

            // build up Blob Connection
            StorageCredentials credential = new StorageCredentialsAccountAndKey(accountName, accountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(credential);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer containerConn = blobClient.getContainerReference(container);
            CloudBlockBlob blob = containerConn.getBlockBlobReference(blobFileName);

            // fetch blob meta info to get blob size
            blob.downloadAttributes();

            // build BlobSourceInfo
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(
                    credential.transformUri(blob.getUri()).toString(), blob.getProperties().getLength());

            // build up IngestionProperties
            IngestionProperties ingestionProperties = new IngestionProperties(kustoDBName, kustoTableName);
            ingestionProperties.setJsonMappingName(jsonMapping);
            ingestionProperties.setDataFormat("json");
            ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
            ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
            ingestionProperties.setFlushImmediately(true);

            // do ingestion and check result
            IngestionResult ingestionResult = client.ingestFromBlob(blobSourceInfo, ingestionProperties);
            OperationStatus status = ingestionResult.GetIngestionStatusCollection().get(0).status;

            while (status == OperationStatus.Pending) {
                // sleep for a while (5s) and do another check if work done
                Thread.sleep(5 * 1000);

                status = ingestionResult.GetIngestionStatusCollection().get(0).status;
            }

            assert (status == OperationStatus.Succeeded);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
