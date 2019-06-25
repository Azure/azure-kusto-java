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

    static private String cluster = "https://<cluster>.kusto.windows.net";
    static private String appId = "";
    static private String appKey = "";
    static private String tenant = "";
    static private String database = "";
    static private String table = "";
    static private String blobContainer = "https://<storage_account>.blob.core.windows.net/<container>/";

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
        IngestionProperties ingestionProperties = new IngestionProperties(database,table);

        // Create Stream from string and Ingest
        String data = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream is = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
        StreamSourceInfo ss = new StreamSourceInfo(is);
        OperationStatus status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        String resourcesDirectorty = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Open CSV File Stream and Ingest
        InputStream fs = new FileInputStream( resourcesDirectorty + "dataset.csv");
        ss.setStream(fs);
        ss.setLeaveOpen(false);
        status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open compressed CSV File Stream and Ingest
        fs = new FileInputStream( resourcesDirectorty + "dataset.csv.gz");
        ss.setStream(fs);
        ss.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open TSV File Stream and Ingest
        ingestionProperties.setDataFormat("tsv");
        fs = new FileInputStream( resourcesDirectorty + "dataset.tsv");
        ss.setStream(fs);
        ss.setIsCompressed(false);
        status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open JSON File Stream and Ingest
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        fs = new FileInputStream(resourcesDirectorty + "dataset.json");
        ss.setStream(fs);
        status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        // Open compressed JSON File Stream and Ingest
        fs = new FileInputStream( resourcesDirectorty + "dataset.jsonz.gz");
        ss.setStream(fs);
        ss.setIsCompressed(true);
        status = streamingIngestClient.ingestFromStream(ss, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    static void IngestFromFile() throws Exception {
        IngestionProperties ingestionProperties = new IngestionProperties(database,table);
        String resourcesDirectorty = System.getProperty("user.dir") + "/samples/src/main/resources/";
        //Ingest CSV file
        String path  = resourcesDirectorty + "dataset.csv";
        FileSourceInfo fis = new FileSourceInfo(path, new File(path).length());
        OperationStatus status = streamingIngestClient.ingestFromFile(fis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest compressed CSV file
        path = resourcesDirectorty + "dataset.csv.gz";
        fis = new FileSourceInfo(path , new File(path).length());
        status = streamingIngestClient.ingestFromFile(fis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest TSV file
        path =  resourcesDirectorty + "dataset.tsv";
        fis = new FileSourceInfo(path , new File(path).length());
        ingestionProperties.setDataFormat("tsv");
        status = streamingIngestClient.ingestFromFile(fis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest JSON file
        path = resourcesDirectorty + "dataset.json";
        fis = new FileSourceInfo(path , new File(path).length());
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        status = streamingIngestClient.ingestFromFile(fis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        //Ingest compressed JSON file
        path = resourcesDirectorty + "dataset.jsonz.gz";
        fis = new FileSourceInfo(path , new File(path).length());
        status = streamingIngestClient.ingestFromFile(fis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }

    static void IngestFromBlob() throws Exception {
        IngestionProperties ingestionProperties = new IngestionProperties(database,table);

        // Ingest CSV blob
        BlobSourceInfo bis = new BlobSourceInfo(blobContainer + "dataset.csv");
        OperationStatus status = streamingIngestClient.ingestFromBlob(bis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        bis = new BlobSourceInfo(blobContainer + "dataset.csv.gz");
        status = streamingIngestClient.ingestFromBlob(bis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        bis = new BlobSourceInfo(blobContainer + "dataset.tsv");
        ingestionProperties.setDataFormat("tsv");
        status = streamingIngestClient.ingestFromBlob(bis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        bis = new BlobSourceInfo(blobContainer + "dataset.json");
        ingestionProperties.setDataFormat("json");
        ingestionProperties.setIngestionMapping("JsonMapping", IngestionMapping.INGESTION_MAPPING_KIND.json);
        status = streamingIngestClient.ingestFromBlob(bis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();

        bis = new BlobSourceInfo(blobContainer + "dataset.jsonz.gz");
        status = streamingIngestClient.ingestFromBlob(bis, ingestionProperties).getIngestionStatusCollection().get(0).status;
        assert status == OperationStatus.Succeeded : "Ingestion failed with status: " + status.toString();
    }
}
