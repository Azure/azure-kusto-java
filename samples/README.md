# Getting Started with Kusto Java SDK

### Prerequisites
- A Java Developer Kit (JDK), version 11 or later
- Maven
- Clone the project and enter the samples directory:
```sh
      git clone https://github.com/Azure/azure-kusto-java.git
      cd samples
```

## Execute Query Sample

This sample will demonstrate how to execute a query.  
[Sample Code](src/main/java/Query.java)

### Prerequisites

- [Create Azure Data Explorer Cluster and DB](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal)
- [Create Azure Active Directory App Registration and grant it permissions to DB](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) (save the app key and the application ID for later)

### Steps to follow

1. Build Connection string and initialize the client

```java
ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
        System.getProperty("clusterPath"),
        System.getProperty("appId"),
        System.getProperty("appKey"),
        System.getProperty("appTenant"));
        Client client = ClientFactory.createClient(csb);
```

If you'd like to tweak the underlying HTTP client used to make the requests, build an HTTP client properties object
and use that along the Connection string to initialise the client:

```java
ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
        System.getProperty("clusterPath"),
        System.getProperty("appId"),
        System.getProperty("appKey"),
        System.getProperty("appTenant"));

        HttpClientProperties properties = HttpClientProperties.builder()
        .keepAlive(true)
        .maxKeepAliveTime(120)
        .maxConnectionsTotal(40)
        .build();

        Client client = ClientFactory.createClient(csb, properties);
```

2. Execute query

```java
KustoOperationResult results = client.execute( System.getProperty("dbName"), System.getProperty("query"));
```

### How to run this sample

```sh
cd samples
mvn clean compile exec:java -Dexec.mainClass="Query" \
                            -DclusterPath="cluster/path" \
                            -DappId="app-id" \
                            -DappKey="appKey" \
                            -DappTenant="subscription-id" \
                            -DdbName="dbName" \
                            -Dquery="your | query"

```

## Advanced Query Sample

This sample shows some more advanced options available when querying data, like using query parameters to guard against injection attacks and extracting individual values from the query results.   
[Sample Code for Advanced Query](src/main/java/AdvancedQuery.java)

### Prerequisites

- [Create Azure Data Explorer Cluster and DB](https://docs.microsoft.com/azure/data-explorer/create-cluster-database-portal)
- [Create Azure Active Directory App Registration and grant it permissions to DB](https://docs.microsoft.com/azure/kusto/management/access-control/how-to-provision-aad-app) (save the app key and the application ID for later). Principal's permission must be at least 'Database user'.

### Notable Features

1. Creating a table with initial data using the [.set-or-replace](https://docs.microsoft.com/azure/data-explorer/kusto/management/data-ingestion/ingest-from-query) command

```java
String tableCommand = String.join(newLine,
        ".set-or-replace Events <|",
        "range x from 1 to 100 step 1",
        "| extend ts = totimespan(strcat(x,'.00:00:00'))",
        "| project timestamp = now(ts), eventName = strcat('event ', x)");
client.execute(database, tableCommand);
```

2. Using query parameters to guard against injection attacks

```java
ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
clientRequestProperties.setParameter("eventNameFilter", "event 1");
String query = String.join(newLine,
        "declare query_parameters(eventNameFilter:string);",
        "Events",
        "| where eventName == eventNameFilter");
KustoOperationResult results = client.execute(database, query, clientRequestProperties);
```

3. Extracting individual values from the query results

```java
KustoResultSetTable mainTableResult = results.getPrimaryResults();
System.out.printf("Kusto sent back %s rows.%n", mainTableResult.count());

// iterate values
List<Event> events = new ArrayList<>();
while (mainTableResult.next()) {
    events.add(new Event(
        mainTableResult.getKustoDateTime("timestamp"), 
        mainTableResult.getString("eventName")));
}
```

### How to run this sample
Note: Running this sample will create a table named `Events` in the given database.
```sh
cd samples
mvn clean compile exec:java -Dexec.mainClass="AdvancedQuery" \
                            -DclusterPath="cluster/path" \
                            -DappId="app-id" \
                            -DappKey="appKey" \
                            -DappTenant="tenant-id" \
                            -DdbName="dbName" 
```

## File Ingestion Sample

This sample will demonstrate how to ingest data from a file into table.  
[Sample Code](src/main/java/FileIngestion.java)

### Prerequisites

- [Create Azure Data Explorer Cluster and DB](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal)
- [Create Azure Active Directory App Registration and grant it permissions to DB](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) (save the app key and the application ID for later)
- [Create a table in the DB](https://docs.microsoft.com/en-us/azure/kusto/management/tables/#create-table)
- [Create a mapping between the file and the table](https://docs.microsoft.com/en-us/azure/kusto/management/tables#create-ingestion-mapping)

### Steps to follow

1. Build connection string and initialize

```java
ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials(System.getProperty("clusterPath"),
                            System.getProperty("appId"),
                            System.getProperty("appKey"),
                            System.getProperty("appTenant"));
```
2. Initialize client and its properties

```java
IngestClient client = IngestClientFactory.createClient(csb);

IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                    System.getProperty("tableName"));
ingestionProperties.getIngestionMapping().setIngestionMappingReference(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.Csv);
```
3. Load file and ingest it into table

```java
FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
```

### How to run this sample

```sh
cd samples
mvn clean compile exec:java -Dexec.cleanupDaemonThreads=false \
                              -Dexec.mainClass="FileIngestion" \
                              -DclusterPath="cluster/path" \
                              -DappId="app-id" \
                              -DappKey="appKey" \
                              -DappTenant="subscription-id" \
                              -DdbName="dbName" \
                              -DtableName="tableName" \
                              -DdataMappingName="dataMappingName" \
                              -DfilePath="file/path"

```

## StreamingIngest Sample

This sample will demonstrate how to ingest data using the streaming ingest client.
{Sample Code](src/main/java/StreamingIngest)

### Prerequisites

- [Create Azure Data Explorer Cluster and DB](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal)
- [Create Azure Active Directory App Registration and grant it permissions to DB](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) (save the app key and the application ID for later)
- [Create a table in the DB](https://docs.microsoft.com/en-us/azure/kusto/management/tables/#create-table):
```kql
.create table StreamingIngest (rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)
```

- [Create a mapping between the file and the table](https://docs.microsoft.com/en-us/azure/kusto/management/tables#create-ingestion-mapping) :
```kql
.create table StreamingIngest ingestion json mapping "JsonMapping" '[{"column":"rownumber","path": "$.rownumber", "datatype":"int" },{"column":"rowguid", "path":"$.rowguid","datatype":"string" },{"column":"xdouble", "path":"$.xdouble", "datatype":"real" },{"column":"xfloat", "path":"$.xfloat", "datatype":"real" },{"column":"xbool", "path":"$.xbool", "datatype":"bool" },{"column":"xint16", "path":"$.xint16", "datatype":"int" },{"column":"xint32", "path":"$.xint32", "datatype":"int" },{"column":"xint64", "path":"$.xint64", "datatype":"long" },{"column":"xuint8", "path":"$.xuint8", "datatype":"long"},{"column":"xuint16", "path":"$.xuint16", "datatype":"long"},{"column":"xuint32", "path":"$.xuint32", "datatype":"long"},{"column":"xuint64", "path":"$.xuint64", "datatype":"long"},{"column":"xdate", "path":"$.xdate", "datatype":"datetime"},{"column":"xsmalltext", "path":"$.xsmalltext","datatype":"string"},{"column":"xtext", "path":"$.xtext","datatype":"string"},{"column":"xnumberAsText", "path":"$.xnumberAsText","datatype":"string"},{"column":"xtime", "path":"$.xtime","datatype":"timespan"}, {"column":"xtextWithNulls", "path":"$.xtextWithNulls","datatype":"string"}, {"column":"xdynamicWithNulls", "path":"$.xdynamicWithNulls","datatype":"dynamic"}]'
```

### Steps to follow

1. Build connection string and initialize

```java
ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials(System.getProperty("clusterPath"),
                            System.getProperty("appId"),
                            System.getProperty("appKey"),
                            System.getProperty("appTenant"));
```
2. Initialize client and its properties

```java
IngestClient client = IngestClientFactory.createClient(csb);

IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                    System.getProperty("tableName"));
```
3. Create Source info

StreamSourceInfo:
```java
InputStream inputStream = new ByteArrayInputStream(Charset.forName("UTF-8").encode(data).array());
StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream);
```
If the data is compressed:
```java
streamSourceInfo.setCompressionType(CompressionType.gz);
```

FileSourceInfo:
```java
FileSourceInfo fileSourceInfo = new FileSourceInfo(path, new File(path).length());
```

4. Ingest into table and verify ingestion status

From stream:
```java
OperationStatus status = streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
```

From File:
```java
OperationStatus status = streamingIngestClient.ingestFromFile(fileSourceInfo, ingestionProperties).getIngestionStatusCollection().get(0).status;
```

### How to run this sample

```sh
cd samples
mvn clean compile exec:java -Dexec.cleanupDaemonThreads=false \
                              -Dexec.mainClass="StreamingIngest" \
                              -DclusterPath="cluster/path" \
                              -DappId="app-id" \
                              -DappKey="appKey" \
                              -DappTenant="subscription-id" \
                              -DdbName="dbName" \
                              -DtableName="tableName" \
                              -DdataMappingName="dataMappingName"
```


### Using CompletableFutures

Take a look at this [Sample Code](src/main/java/FileIngestionCompletableFuture.java) to learn how to run File Ingestion using CompletableFutures in order to make the calls asynchronously.

*_Note: The implementation itself of the File Ingestion API, like all the other APIs in this version, is not asynchronous._*


## Query Table Status Sample

This sample will demonstrate how to retrieve ingestion status.  
[Sample Code](src/main/java/TableStatus.java)

### Prerequisites

- [Create Azure Data Explorer Cluster and DB](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal)
- [Create Azure Active Directory App Registration and grant it permissions to DB](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) (save the app key and the application ID for later)
- [Create a table in the DB](https://docs.microsoft.com/en-us/azure/kusto/management/tables/#create-table)
- [Create a mapping between the file and the table](https://docs.microsoft.com/en-us/azure/kusto/management/tables#create-ingestion-mapping)

### Steps to follow

1. Set timeout for status retrieval :

```java
Integer timeoutInSec = Integer.getInteger("timeoutInSec");
```

2. Build connection string and initialize

```java
ConnectionStringBuilder csb =
        ConnectionStringBuilder.createWithAadApplicationCredentials( System.getProperty("clusterPath"),
                System.getProperty("appId"),
                System.getProperty("appKey"),
                System.getProperty("appTenant"));
```

3. Initialize client and its properties

```java
IngestClient client = IngestClientFactory.createClient(csb);

IngestionProperties ingestionProperties = new IngestionProperties( System.getProperty("dbName"),
                    System.getProperty("tableName"));
            ingestionProperties.getIngestionMapping().setIngestionMappingReference(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.Csv);
            ingestionProperties.setReportMethod(QueueAndTable);
            ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
```

4. Load file and ingest it into table

```java
FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
```

5. Retrieve ingestion status and wait for result

```java
List<IngestionStatus> statuses = ingestionResult.getIngestionStatusCollection();

while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
    Thread.sleep(1000);
    timeoutInSec -= 1;
    statuses = ingestionResult.getIngestionStatusCollection();
}
```

### Running this sample

To run this sample:
```sh
cd samples
mvn clean compile exec:java -Dexec.mainClass="TableStatus" \
                            -DclusterPath="cluster/path" \
                            -DappId="app-id" \
                            -DappKey="appKey" \
                            -DappTenant="subscription-id" \
                            -DdbName="dbName" \
                            -DtableName="tableName" \
                            -DdataMappingName="dataMappingName" \
                            -DfilePath="file/path"
                            -DtimeoutInSec=300
``` 

6. Simple Jmeter sample to test stress test of the clients and cluster

### Running this sample
Fill the right parameters in jmeter_test_load.properties file, the application provided should be
granted User and Ingestor permissions on the database.
The performance we tested here was of the client, and therefore we use the logs to compare 
total run time of each request.
A warn log is logged for each request with and can be parsed as one TXT 
(given column name is 'data') with the following KQL:

jmeterLog
| parse-where data with Datetime:string " WARN " Text:string "after: " ms:int *
| summarize percentiles(ms, 5,80, 90,95),count(), avg(ms) by substring(Text,39)

### More information

[http://azure.com/java](http://azure.com/java)

If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

---

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
