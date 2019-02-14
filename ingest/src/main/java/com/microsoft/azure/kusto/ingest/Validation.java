package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;

public class Validation {

    public static void validateIsNotBlank(String str, String message) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void validateIsNotNull(Object obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void validateFileExists(File file, String message) throws FileNotFoundException {
        validateIsNotNull(file, "file is null");

        if (!file.exists()) {
            throw new FileNotFoundException(message);
        }
    }

    public static void validateFileExists(String filePath) throws FileNotFoundException {
        validateIsNotBlank(filePath, "filePath is blank");

        File file = new File(filePath);
        validateFileExists(file, "file does not exist: " + filePath);
    }

    public static URI validateAndCreateUri(String uri) {
        validateIsNotBlank(uri, "uri is blank");
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("not a valid uri: " + uri);
        }
    }
}
