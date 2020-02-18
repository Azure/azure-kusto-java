import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileIngestion {

    public static void main(String[] args) {
        try {
            Date date = new Date();
            String ClientID ="d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
            String pass = "L+0hoM34kqC22XRniWOgkETwVvawiir2odEjYqZeyXA=";
            String auth = "72f988bf-86f1-41af-91ab-2d7cd011db47";
//            IngestClient  client = IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohbitton.kusto.windows.net"));
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohbitton.dev.kusto.windows.net", ClientID, pass, auth);

            IngestionProperties ingestionProperties = new IngestionProperties("ohtst","TestTable2");
            IngestionResult ingestionResult;
            boolean stream = false;
            if (stream){
                IngestClient  streamclient = IngestClientFactory.createClient(csb);

                ingestionProperties.setDataFormat("Csv");
                ByteArrayOutputStream st = new ByteArrayOutputStream();
                st.write("asd,2".getBytes());
                ByteArrayInputStream byteArrayInputStream = new closeAbleStream(st.toByteArray());
//                InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream);
                StreamSourceInfo info = new StreamSourceInfo(byteArrayInputStream);
//                info.setCompressionType(CompressionType.gz);
                info.setLeaveOpen(true);
                CsvColumnMapping csvColumnMapping = new CsvColumnMapping("ColA", "string", 0);

                ingestionProperties.setIngestionMapping(new ColumnMapping[]{csvColumnMapping}, IngestionMapping.IngestionMappingKind.Csv);

//                ingestionProperties.setIngestionMapping("Csv ._- map", IngestionMapping.IngestionMappingKind.Csv);
                ingestionProperties.setDropByTags(new ArrayList<String>(){{add("asd");}});
                ingestionResult = streamclient.ingestFromStream(info, ingestionProperties);
//                ingestionResult = client.ingestFromStream(info, ingestionProperties);
            }else{
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);

                IngestClient  client = IngestClientFactory.createClient(csb);
                ingestionProperties.setDataFormat("Json");
                JsonColumnMapping jsonColumnMapping = new JsonColumnMapping("a", "string", "$");
                ingestionProperties.setIngestionMapping(new IngestionMapping(new ColumnMapping[]{jsonColumnMapping}, IngestionMapping.IngestionMappingKind.Json));
                //ingestionProperties.setIngestionMapping(new ColumnMapping[]{jsonColumnMapping}, IngestionMapping.IngestionMappingKind.Json);

//                ingestionProperties.setIngestionMapping(new IngestionMapping("TestTable2_mapping", IngestionMapping.IngestionMappingKind.Json));
//                BlobSourceInfo b = new BlobSourceInfo("https://gwjkstrldohbitton01.blob.core.windows.net/ohbittonprivate/existingMappingLvls.json?st=2019-10-22T01%3A52%3A00Z&se=2019-10-23T13%3A52%3A00Z&sp=rl&sv=2018-03-28&sr=b&sig=fNN9yoNVl2Z0BUXGEyBbQJV28sGYxQQjdzC97NDC%2BdI%3D");
               FileSourceInfo f = new FileSourceInfo("C:\\Users\\ohbitton\\Desktop\\demo.Json", 142);
                ingestionResult = client.ingestFromFile( f, ingestionProperties);
                List<IngestionStatus> ingestionStatusCollection = ingestionResult.getIngestionStatusCollection();
                while(ingestionStatusCollection.get(0).status == OperationStatus.Pending)
                {
                    Thread.sleep(5000);
                    ingestionStatusCollection= ingestionResult.getIngestionStatusCollection();

                }
                OperationStatus lastStat = ingestionStatusCollection.get(0).status;
            }
            //TODO ingestion mapping by string !


            System.out.println(ingestionResult);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class closeAbleStream extends ByteArrayInputStream {


    public closeAbleStream(byte[] bytes) {
        super(bytes);
    }

    public void close() throws IOException {
        System.out.println("closed!!");
    }
}