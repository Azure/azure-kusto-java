# Quick Start App

The quick start application is a **self-contained and runnable** example script that demonstrates authenticating, connecting to, administering, ingesting data into and querying Azure Data Explorer using the azure-kusto-python SDK.
You can use it as a baseline to write your own first kusto client application, altering the code as you go, or copy code sections out of it into your app.

**Tip:** The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TODO changes when adapting the code to your needs.


## Using the App for the first time

### Retrieving the app from GitHub
1. Download the app files from this GitHub repo
2. Modify `kusto-sample-config.json`, changing `KustoUri`, `IngestUri` and `DatabaseName` appropriately for your ADX cluster

### Retrieving the app from OneClick
1. Open a browser and type your cluster's URL (e.g. https://myadxcluster.westeurope.kusto.windows.net/), and you will be redirected to the _Azure Data Explorer_ website
2. Open the left-side pane via the hamburger menu, if it isn't already open
3. On the left-side pane, choose _Data_
4. Click on _Generate Sample App Code_ button
5. Follow the wizard
6. Download the app as a zip file
7. Unpack the script to your folder of choice
8. The configuration parameters defined in `kusto-sample-config.json` are already defined appropriately for your ADX cluster, which you can verify

### Running the app
1. Open a command line window to the folder downloaded above
2. Run `mvn clean install`
3. Run `java target\kusto-quickstart-[version]-jar-with-dependencies.jar`

### Optional Changes
1. Within the script itself, you may alter the default User-Prompt authentication method by editing `authenticationMode`
2. You can also make the script run without stopping between steps by setting `waitForUser = False`

###Troubleshooting
* If you are having trouble running Java on your machine or need instructions on how to install Java, you can consult a Java environment setup tutorial, like [this one](https://www.tutorialspoint.com/java/java_environment_setup.htm).
* If you are having trouble running the script from your IDE of choice, first check if the script runs from command line, then consult the troubleshooting references of your IDE.