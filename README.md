# Microsoft Azure Kusto SDK for Java

master: [![Build Status](https://travis-ci.org/Azure/azure-kusto-java.svg?branch=master)](https://travis-ci.org/Azure/azure-kusto-java) 
dev: [![Build Status](https://travis-ci.org/Azure/azure-kusto-java.svg?branch=dev)](https://travis-ci.org/Azure/azure-kusto-java)

This is the Microsoft Azure Kusto client library which allows communication with Kusto to bring data in (ingest) and query information already stored in the database.
This library contains 3 different modules:
- data: the main client that allows interaction with Kusto. It's able to create a connection, issue (control) commands and query data.
- ingest: this provides an easy way to bring data into Kusto
- samples 

# Install

Currently, you will need to download the source code, compile and install locally.
One way to do this is by using maven like in the following example:
```
git clone git://github.com/Azure/azure-kusto-java.git
cd azure-kusto-java
mvn install
```

In the near future this library will be available directly off Maven.

# Prerequisites

- A Java Developer Kit (JDK), version 1.8 or later
- Maven

# Samples

[Code Samples](samples/README.md)

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
