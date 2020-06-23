// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

class SecurityUtils {
    static String removeSecretsFromUrl(String url) {
        return url.split("[?]", 2)[0].split("[;]", 2)[0];
    }
}
