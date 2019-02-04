package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;

public class ValidationHelper {

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
        if (!file.exists()) {
            throw new FileNotFoundException(message);
        }
    }

    public static File validateFileExists(String filePath, String message) throws FileNotFoundException {
        File file = new File(filePath);
        validateFileExists(file, message);
        return file;
    }
}
