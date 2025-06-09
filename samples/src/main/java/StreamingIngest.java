// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.StreamingIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class StreamingIngest {

    private static String database;
    private static String table;
    private static String mapping;
    private static StreamingIngestClient streamingIngestClient;

    public static void main(String[] args) {
        try {
            // ingest-
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"), // "https://<cluster>.kusto.windows.net"
                    System.getProperty("app-id"),
                    System.getProperty("appKey"),
                    System.getProperty("tenant"));
            streamingIngestClient = IngestClientFactory.createStreamingIngestClient(csb);

            database = System.getProperty("dbName");
            table = System.getProperty("tableName");
            mapping = System.getProperty("dataMappingName");

            ingestFromStream();
            ingestFromFile();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void ingestFromStream() throws IngestionClientException, IngestionServiceException, FileNotFoundException, URISyntaxException {
        IngestionProperties ingestionProperties = new IngestionProperties(database, table);

        // Create Stream from string and Ingest
        String data = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream inputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(data).array());
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
        OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollectionAsync().block()
                .get(0).status;
        System.out.println(status.toString());

        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";

        // Open compressed CSV File Stream and Ingest
        FileInputStream fileInputStream = new FileInputStream(resourcesDirectory + "dataset.csv.gz");
        streamSourceInfo.setStream(fileInputStream);
        /*
         * In order to make efficient ingestion requests, the streaming ingest client compress the given stream unless it is already compressed. When the given
         * stream content is already compressed, we should set this property true to avoid double compression.
         */
        streamSourceInfo.setCompressionType(CompressionType.gz);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollectionAsync().block().get(0).status;
        System.out.println(status.toString());

        // Open JSON File Stream and Ingest
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping(mapping, IngestionMapping.IngestionMappingKind.JSON);
        fileInputStream = new FileInputStream(resourcesDirectory + "dataset.json");
        streamSourceInfo.setStream(fileInputStream);
        status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollectionAsync().block().get(0).status;
        System.out.println(status.toString());
    }

    static void ingestFromFile() throws IngestionClientException, IngestionServiceException, URISyntaxException {
        IngestionProperties ingestionProperties = new IngestionProperties(database, table);
        String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
        // Ingest CSV file
        String path = resourcesDirectory + "dataset.csv";
        FileSourceInfo fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
        OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollectionAsync().block()
                .get(0).status;
        System.out.println(status.toString());

        // Ingest compressed JSON file
        path = resourcesDirectory + "dataset.jsonz.gz";
        fileSourceInfo = new FileSourceInfo(path);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        ingestionProperties.setIngestionMapping(mapping, IngestionMapping.IngestionMappingKind.JSON);
        status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollectionAsync().block().get(0).status;
        System.out.println(status.toString());
    }
}
