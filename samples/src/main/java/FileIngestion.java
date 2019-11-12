import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.*;
import java.util.ArrayList;

public class FileIngestion {

    public static void main(String[] args) {
        try {
            String ClientID ="d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
            String pass = "L+0hoM34kqC22XRniWOgkETwVvawiir2odEjYqZeyXA=";
            String auth = "72f988bf-86f1-41af-91ab-2d7cd011db47";
//            IngestClient  client = IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohbitton.kusto.windows.net"));
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohbitton.dev.kusto.windows.net", ClientID, pass, auth);

            IngestionResult ingestionResult;
            boolean stream = false;
            if (stream){
                IngestionProperties ingestionProperties = new IngestionProperties("ohtst","asd");

                IngestClient  streamclient = IngestClientFactory.createClient(csb);

                ingestionProperties.setDataFormat("csv");
                ByteArrayOutputStream st = new ByteArrayOutputStream();
                st.write("asd,2".getBytes());
                ByteArrayInputStream byteArrayInputStream = new closeAbleStream(st.toByteArray());
//                InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream);
                StreamSourceInfo info = new StreamSourceInfo(byteArrayInputStream);
//                info.setCompressionType(CompressionType.gz);
                info.setLeaveOpen(true);
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);

//                ingestionProperties.setIngestionMapping("csv ._- map", IngestionMapping.IngestionMappingKind.csv);
                ingestionProperties.setDropByTags(new ArrayList<String>(){{add("asd");}});
                ingestionResult = streamclient.ingestFromStream(info, ingestionProperties);
//                ingestionResult = client.ingestFromStream(info, ingestionProperties);
            }else{
                IngestionProperties ingestionProperties = new IngestionProperties("the what","allDataTypes");
                IngestClient  client = IngestClientFactory.createClient(csb);
                ingestionProperties.setDataFormat("csv");
//                ingestionProperties.setIngestionMapping(new IngestionMapping("lvl0", IngestionMapping.IngestionMappingKind.json));
//                BlobSourceInfo b = new BlobSourceInfo("https://gwjkstrldohbitton01.blob.core.windows.net/ohbittonprivate/existingMappingLvls.json?st=2019-10-22T01%3A52%3A00Z&se=2019-10-23T13%3A52%3A00Z&sp=rl&sv=2018-03-28&sr=b&sig=fNN9yoNVl2Z0BUXGEyBbQJV28sGYxQQjdzC97NDC%2BdI%3D");
//                ingestionResult = client.ingestFromBlob(b , ingestionProperties);
                FileSourceInfo file = new FileSourceInfo("C:\\Users\\ohbitton\\Desktop\\allDataTypes.csv",113);
                ingestionResult = client.ingestFromFile(file , ingestionProperties);

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