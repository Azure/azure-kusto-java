package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

public class UriUtils {
    private UriUtils() {
        // Providing hidden constructor to hide default public constructor in utils class
    }

    public static String setPathForUri(String uri, String path, boolean ensureTrailingSlash) throws URISyntaxException {
        path = StringUtils.prependIfMissing(path, "/");

        String pathString = new URIBuilder(uri).setPath(path).build().toString();
        if (ensureTrailingSlash) {
            pathString = StringUtils.appendIfMissing(pathString, "/");
        }
        return pathString;
    }

    public static String setPathForUri(String uri, String path) throws URISyntaxException {
        return setPathForUri(uri, path, false);
    }
}
