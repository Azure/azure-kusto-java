package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;

public class ValidationHelper {

    public static void validateIsNotEmpty(String str, String message) {
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
}
