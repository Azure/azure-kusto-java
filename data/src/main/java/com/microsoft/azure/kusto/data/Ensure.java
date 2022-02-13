// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.io.File;
import java.io.FileNotFoundException;
import org.apache.commons.lang3.StringUtils;

public class Ensure {

    public static void stringIsNotBlank(String str, String varName) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(varName + " is blank.");
        }
    }

    public static void argIsNotNull(Object arg, String varName) {
        if (arg == null) {
            throw new IllegalArgumentException(varName + " is null.");
        }
    }

    public static void fileExists(File file, String argFile) throws FileNotFoundException {
        argIsNotNull(file, argFile);

        if (!file.exists()) {
            throw new FileNotFoundException(argFile);
        }
    }

    public static void fileExists(String filePath) throws FileNotFoundException {
        stringIsNotBlank(filePath, "filePath");

        File file = new File(filePath);
        fileExists(file, filePath);
    }

    public static void isTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Condition evaluated to false: " + message);
        }
    }

    public static void isFalse(boolean condition, String message) {
        if (condition) {
            throw new IllegalArgumentException("Condition evaluated to True: " + message);
        }
    }
}
