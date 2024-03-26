package com.microsoft.azure.kusto.data;

/** An extensible wrapper class representing a Kusto Query or Command. */
public class KustoQuery {

    private String command;
    private String database;
    private ClientRequestProperties properties;

    /**
     * A constructor providing only the command to be executed.
     * @param command the command to be executed
     */
    public KustoQuery(String command) {
        this.command = command;
    }

    /**
     * A constructor providing the command to be executed and the database to target.
     * @param command the command to be executed
     * @param database the name of the database to target
     */
    public KustoQuery(String command, String database) {
        this.command = command;
        this.database = database;
    }

    /**
     * A constructor providing the command to be executed and sanitized query parameters.
     * @param command the command to be executed
     * @param properties a map of query parameters and other request properties
     */
    public KustoQuery(String command, ClientRequestProperties properties) {
        this.command = command;
        this.properties = properties;
    }

    /**
     * A constructor providing the command to be executed, sanitized query parameters, and the database to target.
     * @param command the command to be executed
     * @param database the name of the database to target
     * @param properties a map of query parameters and other request properties
     */
    public KustoQuery(String command, String database, ClientRequestProperties properties) {
        this.command = command;
        this.database = database;
        this.properties = properties;
    }

    /**
     * A getter for this KustoQuery object's inner command String.
     * @return the command
     */
    public String getCommand() {
        return command;
    }

    /**
     * A setter for this KustoQuery object's inner command String.
     * @param command the command
     */
    public void setCommand(String command) {
        this.command = command;
    }

    /**
     * A getter for this KustoQuery object's inner database String.
     * @return the database name
     */
    public String getDatabase() {
        return database;
    }

    /**
     * A setter for this KustoQuery object's inner database String.
     * @param database the database name
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * A getter for this KustoQuery object's inner ClientRequestProperties.
     * @return the properties
     */
    public ClientRequestProperties getProperties() {
        return properties;
    }

    /**
     * A setter for this KustoQuery object's inner ClientRequestProperties.
     * @param properties the properties
     */
    public void setProperties(ClientRequestProperties properties) {
        this.properties = properties;
    }
}
