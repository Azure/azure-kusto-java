# Getting Started with Kusto Java SDK

### Prerequisites
  - A Java Developer Kit (JDK), version 1.8 or later
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
ClientImpl client = new ClientImpl(csb);
```

2. Execute query

```java
Results results = client.execute( System.getProperty("dbName"), System.getProperty("query"));
```

### How to run this sample

```sh
mvn clean compile exec:java -Dexec.mainClass="Query" \
                            -DclusterPath="cluster/path" \
                            -DappId="app-id" \
                            -DappKey="appKey" \
                            -DappTenant="subscription-id" \
                            -DdbName="dbName" \
                            -Dquery="your | query"

```

## File Ingestion Sample

This sample will demonstrate how to ingest data from file into table.  
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
2. Initialize client and it's properties

```java
IngestClient client = IngestClientFactory.createClient(csb);

IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                    System.getProperty("tableName"));
ingestionProperties.setJsonMappingName(System.getProperty("dataMappingName"));
```
3. Load file and ingest it into table

```java
FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
```

### How to run this sample

```sh
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

3. Initialize client and it's properties

```java
IngestClient client = IngestClientFactory.createClient(csb);

IngestionProperties ingestionProperties = new IngestionProperties( System.getProperty("dbName"),
                    System.getProperty("tableName"));
            ingestionProperties.setJsonMappingName(System.getProperty("dataMappingName"));
            ingestionProperties.setReportMethod(QueueAndTable);
            ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
```

4. Load file and ingest it into table

```java
FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
```

5. Retrieve ingestion status and wait for result

```java
List<IngestionStatus> statuses = ingestionResult.GetIngestionStatusCollection();

while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
    Thread.sleep(1000);
    timeoutInSec -= 1;
    statuses = ingestionResult.GetIngestionStatusCollection();
}
```

### Running this sample

To run this sample:
```sh
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

### More information 

[http://azure.com/java](http://azure.com/java)

If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

---

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.