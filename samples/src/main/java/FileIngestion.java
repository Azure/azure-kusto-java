import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;

public class FileIngestion {

    private static final String appId = "2cb0cc4c-c9be-4301-b2aa-718935b0ce1d";
    private static final String appKey = "8puj2AAlrkd+zNOIA9Zi303bMuQ3SCDq6Wgn4SXmUlc=";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-csetests.westeurope.kusto.windows.net";
            String dbName = "raabusal";
            String tableName = "test1";
            String dataMappingName = "map1";
            String dataFormat = "json";

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath,appId,appKey,"72f988bf-86f1-41af-91ab-2d7cd011db47");
            IngestionProperties ingestionProperties = new IngestionProperties(dbName,tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            IngestClient client = IngestClientFactory.createClient(kcsb);


            String filePath = "C:\\Development\\temp\\kusto test data\\testdata1.json";

            Timer timer = new Timer(true);

            TimerTask sendFileTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        System.out.println(System.currentTimeMillis() + " Sending file. Thread_ID: " + Thread.currentThread().getId());
                        InputStream stream = new FileInputStream(filePath);
                        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false);
                        client.ingestFromStream(streamSourceInfo,ingestionProperties);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            timer.schedule(sendFileTask, 0, 1000 * 25);

            Thread.sleep(1000*60*10);
//
//            stream = new FileInputStream(filePath);
//            client.ingestFromStream(stream,ingestionProperties,false);


//            String filePath = "C:\\Development\\temp\\kusto test data\\testdata";
//            for(int i = 1; i<11; i++){
//                client.ingestFromSingleFile(String.format("%s%d.json", filePath, i), ingestionProperties);
//                Thread.sleep(5000);
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}