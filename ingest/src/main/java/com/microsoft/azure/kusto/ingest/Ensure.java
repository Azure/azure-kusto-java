package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;

public class Ensure {

    public static void stringIsNotBlank(String str, String message) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void argIsNotNull(Object arg, String message) {
        if (arg == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void fileExists(File file, String message) throws FileNotFoundException {
        argIsNotNull(file, "file is null");

        if (!file.exists()) {
            throw new FileNotFoundException(message);
        }
    }

    public static void fileExists(String filePath) throws FileNotFoundException {
        stringIsNotBlank(filePath, "filePath is blank");

        File file = new File(filePath);
        fileExists(file, "file does not exist: " + filePath);
    }

    public static URI validateAndCreateUri(String uri) {
        stringIsNotBlank(uri, "uri is blank");
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("not a valid uri: " + uri);
        }
    }
}
