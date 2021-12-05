### Prerequisites
1. Set up Java on your machine. For instructions, consult a Java environment setup tutorial, like [this one](https://www.tutorialspoint.com/java/java_environment_setup.htm).
2. Set up [Apache Maven](https://maven.apache.org/install.html), which is the most popular Java dependency management tool.

### Instructions
1. Download the **DOWNLOAD_LINK** as a zip file
2. Extract the app source code to your folder of choice
3. Open a command line window to the folder extracted to above
4. Run `mvn clean install` to compile the source code into a binary
5. Run the binary using `java -jar target\kusto-quickstart-[version]-jar-with-dependencies.jar`

### Troubleshooting

* If you are having trouble running the script from your IDE, first check if the script runs from command line, then consult the troubleshooting references of your IDE.