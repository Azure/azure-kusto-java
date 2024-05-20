package com.microsoft.azure.kusto.data.req;

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.CommandType;
import org.apache.commons.lang3.StringUtils;

public class KustoQuery {

    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";

    private String command;
    private CommandType commandType;
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
     * A constructor providing the command to be executed, and the command type.
     * @param command the command to be executed
     * @param commandType the command type
     */
    public KustoQuery(String command, CommandType commandType) {
        this.command = command;
        this.commandType = commandType;
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
     * A constructor providing the command to be executed, the database to target, and the command type.
     * @param command the command to be executed
     * @param database the name of the database to target
     * @param commandType the command type
     */
    public KustoQuery(String command, String database, CommandType commandType) {
        this.command = command;
        this.database = database;
        this.commandType = commandType;
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
     * A constructor providing the command to be executed, the database to target, and the command type.
     * @param command the command to be executed
     * @param properties a map of query parameters and other request properties
     * @param commandType the command type
     */
    public KustoQuery(String command, ClientRequestProperties properties, CommandType commandType) {
        this.command = command;
        this.properties = properties;
        this.commandType = commandType;
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
     * An all args constructor providing the command to be executed, sanitized query parameters, the database to target, and the command type.
     * @param command the command to be executed
     * @param database the name of the database to target
     * @param properties a map of query parameters and other request properties
     * @param commandType the command type
     */
    public KustoQuery(String command, String database, ClientRequestProperties properties, CommandType commandType) {
        this.command = command;
        this.database = database;
        this.properties = properties;
        this.commandType = commandType;
    }

    /**
     * A getter for this KustoRequest object's inner command String.
     * @return the command
     */
    public String getCommand() {
        return command;
    }

    /**
     * A setter for this KustoRequest object's inner command String.
     * @param command the command
     */
    public void setCommand(String command) {
        this.command = command;
    }

    /**
     * A getter for this KustoRequest object's inner command type.
     * @return the command type
     */
    public CommandType getCommandType() {
        return commandType;
    }

    /**
     * A setter for this KustoRequest object's inner command type.
     * @param commandType the command type
     */
    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    /**
     * A getter for this KustoRequest object's inner database String.
     * @return the database name
     */
    public String getDatabase() {
        return database;
    }

    /**
     * A setter for this KustoRequest object's inner database String.
     * @param database the database name
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * A getter for this KustoRequest object's inner ClientRequestProperties.
     * @return the properties
     */
    public ClientRequestProperties getProperties() {
        return properties;
    }

    /**
     * A setter for this KustoRequest object's inner ClientRequestProperties.
     * @param properties the properties
     */
    public void setProperties(ClientRequestProperties properties) {
        this.properties = properties;
    }

    /** Validates and optimizes the KustoQuery object. */
    public void validateAndOptimize() {
        if (database == null) {
            database = DEFAULT_DATABASE_NAME;
        }
        // Argument validation
        if (StringUtils.isEmpty(database)) {
            throw new IllegalArgumentException("Database is empty");
        }
        if (StringUtils.isEmpty(command)) {
            throw new IllegalArgumentException("Command is empty");
        }
        // Optimize the command by removing superfluous whitespace
        command = command.trim();
        if (commandType == null) {
            commandType = determineCommandType(command);
        }
    }

    private CommandType determineCommandType(String command) {
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            return CommandType.ADMIN_COMMAND;
        }
        return CommandType.QUERY;
    }
    
}