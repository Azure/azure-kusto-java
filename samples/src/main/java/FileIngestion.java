import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.HashMap;

public class FileIngestion {

    public static void main(String[] args) {
        try {

            Date date = new Date();
            String ClientID ="d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
            String pass = "L+0hoM34kqC22XRniWOgkETwVvawiir2odEjYqZeyXA=";
            String auth = "72f988bf-86f1-41af-91ab-2d7cd011db47";
//            IngestClient  client = IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net"));
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net", ClientID, pass, auth);
            IngestClient  client = IngestClientFactory.createClient(csb);

            IngestionProperties ingestionProperties = new IngestionProperties("ohtst","ddd");


            ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.json);

            FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\Desktop\\csv.csv", 8);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
            ByteArrayOutputStream st = new ByteArrayOutputStream();
            st.write("asd,2".getBytes());
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(st.toByteArray());
            StreamSourceInfo info = new StreamSourceInfo(byteArrayInputStream);

            // Ingest with inline ingestion mapping - less recommended
            IngestionProperties ingestionProperties2 = new IngestionProperties("ohtst","ddd");

            ColumnMapping csvColumnMapping = new ColumnMapping("d", "string", new HashMap<String,String>(){{
                put(MappingConst.ORDINAL.name(), "0");
            }});
            ColumnMapping csvColumnMapping2 = new ColumnMapping("[1]", "int", new HashMap<String,String>(){{
                put(MappingConst.ORDINAL.name(), "1");
            }});
            ingestionProperties2.setDataFormat("csv");
            ingestionProperties2.setIngestionMapping(new ColumnMapping[]{csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.csv);
            IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}