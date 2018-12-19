# Microsoft Azure Kusto (Azure Data Explorer) SDK for Java

master: [![Build Status](https://travis-ci.org/Azure/azure-kusto-java.svg)](https://travis-ci.org/Azure/azure-kusto-java) 
dev: [![Build Status](https://travis-ci.org/Azure/azure-kusto-java.svg?branch=dev)](https://travis-ci.org/Azure/azure-kusto-java)

This is the Microsoft Azure Kusto client library which allows communication with Kusto to bring data in (ingest) and query information already stored in the database.
This library contains 3 different modules:
- data: the main client that allows interaction with Kusto. It's able to create a connection, issue (control) commands and query data.
- ingest: this provides an easy way to bring data into Kusto
- samples 

# Install


## Clone
Download the source code, compile and install locally.

One way to do this is by using maven like in the following example:
```
git clone git://github.com/Azure/azure-kusto-java.git
cd azure-kusto-java
mvn install
```
## JitPack
Using [JitPack](https://jitpack.io/), you can point your package manager to a git repo.
For example using maven, you need to add JitPack as a repository:

```xml
<repositories>
  <repository>
      <id>jitpack.io</id>
      <url>https://jitpack.io</url>
  </repository>
</repositories>
```

Afterwhich you will able to include it using maven:

```xml
<dependency>
  <groupId>com.github.Azure</groupId>
  <artifactId>azure-kusto-java</artifactId>
  <version>v0.2.0</version>
</dependency>
```

## Maven

In the near future this library will be available directly off Maven.

# Prerequisites

- A Java Developer Kit (JDK), version 1.8 or later
- Maven

# Samples

- [Execute a query](samples/README.md#execute-query-sample)
- [Ingest a file](samples/README.md#file-ingestion-sample)
- [Check status of an ingest operation](samples/README.md#query-table-status-sample)

# Need Support?
- **Have a feature request for SDKs?** Please post it on [User Voice](https://feedback.azure.com/forums/915733-azure-data-explorer) to help us prioritize
- **Have a technical question?** Ask on [Stack Overflow with tag "azure-data-explorer"](https://stackoverflow.com/questions/tagged/azure-data-explorer)
- **Need Support?** Every customer with an active Azure subscription has access to [support](https://docs.microsoft.com/en-us/azure/azure-supportability/how-to-create-azure-support-request) with guaranteed response time.  Consider submitting a ticket and get assistance from Microsoft support team
- **Found a bug?** Please help us fix it by thoroughly documenting it and [filing an issue](https://github.com/Azure/azure-kusto-python/issues/new).

# Looking for SDKs for other languages/platforms?
- [Node](https://github.com/azure/azure-kusto-node)
- [Python](https://github.com/azure/azure-kusto-python)
- [.NET](https://docs.microsoft.com/en-us/azure/kusto/api/netfx/about-the-sdk)

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
