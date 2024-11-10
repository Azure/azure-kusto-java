package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;

public class UriUtils {

    public static final String FEDERATED_SECURITY_SUFFIX = ";fed=true";

    private UriUtils() {
        // Providing hidden constructor to hide default public constructor in utils class
    }

    public static String createClusterURLFrom(final String clusterURI) throws URISyntaxException {
        URI clusterUrlForParsing = new URI(clusterURI);
        String host = clusterUrlForParsing.getHost();
        Objects.requireNonNull(clusterUrlForParsing.getAuthority(), "clusterUri must have uri authority component");
        String auth = clusterUrlForParsing.getAuthority().toLowerCase();
        if (host == null) {
            host = StringUtils.removeEndIgnoreCase(auth, FEDERATED_SECURITY_SUFFIX);
        }

        String path = clusterUrlForParsing.getPath();
        if (path != null && !path.isEmpty()) {
            path = StringUtils.removeEndIgnoreCase(path, FEDERATED_SECURITY_SUFFIX);
            path = StringUtils.removeEndIgnoreCase(path, "/");
        }

        String clusterUri = String.format("%s://%s%s%s",
                clusterUrlForParsing.getScheme(),
                host,
                clusterUrlForParsing.getPort() != -1 ? ":" + clusterUrlForParsing.getPort() : StringUtils.EMPTY,
                path);
        return new URI(clusterUri).toString();
    }

    public static String setPathForUri(String uri, String path, boolean ensureTrailingSlash) throws URISyntaxException {
        path = StringUtils.prependIfMissing(path, "/");

        URI baseUri = new URI(uri);

        URI newUri = new URI(
                baseUri.getScheme(),
                baseUri.getAuthority(),
                path,
                baseUri.getQuery(),
                baseUri.getFragment());
        String pathString = newUri.toString();
        if (ensureTrailingSlash) {
            pathString = StringUtils.appendIfMissing(pathString, "/");
        }

        return pathString;
    }

    public static String setPathForUri(String uri, String path) throws URISyntaxException {
        return setPathForUri(uri, path, false);
    }

    public static String appendPathToUri(String uri, String path) throws URISyntaxException {
        String existing = new URI(uri).getPath();
        return setPathForUri(uri, StringUtils.appendIfMissing(existing == null ? "" : existing, "/") + path);
    }

    public static boolean isLocalAddress(String host) {
        if (host.equals("localhost")
                || host.equals("127.0.0.1")
                || host.equals("::1")
                || host.equals("[::1]")) {
            return true;
        }

        if (host.startsWith("127.") && host.length() <= 15 && host.length() >= 9) {
            for (int i = 0; i < host.length(); i++) {
                char c = host.charAt(i);
                if (c != '.' && (c < '0' || c > '9')) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    public static String removeExtension(String filename) {
        if (filename == null) {
            return null;
        }
        int extensionPos = filename.lastIndexOf('.');
        int lastDirSeparator = filename.lastIndexOf(File.separatorChar);
        if (extensionPos == -1 || lastDirSeparator > extensionPos) {
            return filename;
        } else {
            return filename.substring(lastDirSeparator + 1, extensionPos);
        }
    }

    public static String[] getSasAndEndpointFromResourceURL(String url) throws URISyntaxException {
        String[] parts = url.split("\\?");

        if (parts.length != 2) {
            throw new URISyntaxException(url, "URL is missing the required SAS query");
        }
        return parts;
    }

    // Given a cmd line used to run the java app, this method strips out the file name running
    // i.e: "home/user/someFile.jar -arg1 val" -> someFile
    public static String stripFileNameFromCommandLine(String cmdLine) {
        try {
            String processNameForTracing = cmdLine;

            if (processNameForTracing != null) {
                processNameForTracing = Paths.get(processNameForTracing.trim().split(" ")[0]).getFileName().toString();
            }

            return processNameForTracing;
        } catch (Exception e) {
            return null;
        }

    }
}
