import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.StreamingIngestClient;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

import java.io.*;
import java.nio.charset.Charset;

public class StreamingIngest {

    static private final String cluster = "https://<cluster>.kusto.windows.net";
    static private final String appId = "";
    static private final String appKey = "";
    static private final String tenant = "";
    static private final String database = "";
    static private final String table = "";
    static private final String blobContainer = "https://<storage_account>.blob.core.windows.net/<container>/";

    private static ConnectionStringBuilder csb;
    private static StreamingIngestClient streamingIngestClient;

    public static void main(String[] args) {
        try {
            csb = ConnectionStringBuilder.createWithAadApplicationCredentials(cluster, appId, appKey, tenant);
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(csb);
            IngestFromStream();
            IngestFromFile();
            IngestFromBlob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void IngestFromStream() throws Exception {
        IngestionProperties ingestionProperties = new IngestionProperties(database, table);

        // Create Stream from string and Ingest
        String data = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Open CSV File Stream and Ingest
        FileInputStream fileInputStream = new FileInputStream(resourcesDirectory + "dataset.csv");
        streamSourceInfo.setStream(fileInputStream);
        streamSourceInfo.setLeaveOpen(false);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open compressed CSV File Stream and Ingest
        fileInputStream = new FileInputStream(resourcesDirectory + "dataset.csv.gz");
        streamSourceInfo.setStream(fileInputStream);
        streamSourceInfo.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open TSV File Stream and Ingest
        ingestionProperties.setDataFormat("tsv");
        fileInputStream = new FileInputStream(resourcesDirectory + "dataset.tsv");
        streamSourceInfo.setStream(fileInputStream);
        streamSourceInfo.setIsCompressed(false);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open JSON File Stream and Ingest
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        fileInputStream = new FileInputStream(resourcesDirectory + "dataset.json");
        streamSourceInfo.setStream(fileInputStream);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open compressed JSON File Stream and Ingest
        fileInputStream = new FileInputStream(resourcesDirectory + "dataset.jsonz.gz");
        streamSourceInfo.setStream(fileInputStream);
        streamSourceInfo.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    static void IngestFromFile() throws Exception {
        IngestionProperties ingestionProperties = new IngestionProperties(database, table);
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        //Ingest CSV file
        String path = resourcesDirectory + "dataset.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest compressed CSV file
        path = resourcesDirectory + "dataset.csv.gz";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest TSV file
        path = resourcesDirectory + "dataset.tsv";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat("tsv");
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest JSON file
        path = resourcesDirectory + "dataset.json";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest compressed JSON file
        path = resourcesDirectory + "dataset.jsonz.gz";
        fileSourceInfo = new FileSourceInfo(path, new File(path).length());
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    static void IngestFromBlob() throws Exception {
        IngestionProperties ingestionProperties = new IngestionProperties(database, table);

        // Ingest CSV blob
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobContainer + "dataset.csv");
        OperationStatus status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        blobSourceInfo = new BlobSourceInfo(blobContainer + "dataset.csv.gz");
        status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        blobSourceInfo = new BlobSourceInfo(blobContainer + "dataset.tsv");
        ingestionProperties.setDataFormat("tsv");
        status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        blobSourceInfo = new BlobSourceInfo(blobContainer + "dataset.json");
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        blobSourceInfo = new BlobSourceInfo(blobContainer + "dataset.jsonz.gz");
        status = streamingIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }
}
