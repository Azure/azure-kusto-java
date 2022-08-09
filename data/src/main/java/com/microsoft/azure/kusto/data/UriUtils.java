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

    public static boolean isLocalAddress(String host)
    {
        if (host.equals("localhost")
                || host.equals("127.0.0.1")
                || host.equals("::1")
                || host.equals("[::1]"))
        {
            return true;
        }

        if (host.startsWith("127.") && host.length() <= 15 && host.length() >= 9)
        {
            for (int i = 0; i < host.length(); i++) {
                char c = host.charAt(i);
                if (c != '.' && (c < '0' || c > '9'))
                {
                    return false;
                }
            }
            return true;
        }

        return false;
    }
}
