package com.microsoft.azure.kusto.data;

import com.azure.core.util.CoreUtils;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClientDetails {

    public static final String NONE = "[none]";

    private static final ConcurrentHashMap<String, String> defaultValues = new ConcurrentHashMap<>();
    public static final String DEFAULT_APPLICATION = "defaultApplication";
    public static final String DEFAULT_USER = "defaultUser";
    public static final String DEFAULT_VERSION = "defaultVersion";

    private final String applicationForTracing;
    private final String userNameForTracing;
    private final String appendedClientVersionForTracing;

    public ClientDetails(String applicationForTracing, String userNameForTracing, String appendedClientVersionForTracing) {
        this.applicationForTracing = applicationForTracing;
        this.userNameForTracing = userNameForTracing;
        this.appendedClientVersionForTracing = appendedClientVersionForTracing;
    }

    private static String unpackLazy(String key) {
        if(DEFAULT_APPLICATION.equalsIgnoreCase(key)) {
            return defaultValues.computeIfAbsent(key,
                    k->UriUtils.stripFileNameFromCommandLine(System.getProperty("sun.java.command")));
        } else if(DEFAULT_USER.equalsIgnoreCase(key)) {
            return defaultValues.computeIfAbsent(key, k -> {
                String user = System.getProperty("user.name");
                if (CoreUtils.isNullOrEmpty(user)) {
                    user = System.getenv("USERNAME");
                    String domain = System.getenv("USERDOMAIN");
                    if (!CoreUtils.isNullOrEmpty(domain) && !CoreUtils.isNullOrEmpty(user)) {
                        user = domain + "\\" + user;
                    }
                }
                return !CoreUtils.isNullOrEmpty(user) ? user : NONE;
            });
        } else if(DEFAULT_VERSION.equalsIgnoreCase(key)) {
            return defaultValues.computeIfAbsent(key, k -> {
                Map<String,String> baseMap = new LinkedHashMap<>();
                baseMap.put("Kusto.Java.Client", Utils.getPackageVersion());
                baseMap.put(String.format("Runtime.%s", escapeField(getRuntime())), getJavaVersion());
                return formatHeader(baseMap);
            });
        } else {
            return NONE;
        }
    }

    /**
     * Formats the given fields into a string that can be used as a header.
     *
     * @param args The fields to format.
     * @return The formatted string, for example: "field1:{value1}|field2:{value2}"
     */
    private static String formatHeader(Map<String, String> args) {
        return args.entrySet().stream().
                filter(arg -> !CoreUtils.isNullOrEmpty(arg.getKey())
                        && !CoreUtils.isNullOrEmpty(arg.getValue()))
                .map(arg -> String.format("%s:%s", arg.getKey(), escapeField(arg.getValue())))
                .collect(Collectors.joining("|"));
    }

    private static String escapeField(String field) {
        return String.format("{%s}", field.replaceAll("[\\r\\n\\s{}|]+", "_"));
    }

    /**
     * Sets the application name and username for Kusto connectors.
     *
     * @param name             The name of the connector/application.
     * @param version          The version of the connector/application.
     * @param sendUser         True if the user should be sent to Kusto, otherwise "[none]" will be sent.
     * @param overrideUser     The user to send to Kusto, or null zvto use the current user.
     * @param appName          The app hosting the connector, or null to use the current process name.
     * @param appVersion       The version of the app hosting the connector, or null to use "[none]".
     * @param additionalFields Additional fields to trace.
     *                         Example: "Kusto.MyConnector:{1.0.0}|App.{connector}:{0.5.3}|Kusto.MyField:{MyValue}"
     */
    public static ClientDetails fromConnectorDetails(String name, String version, boolean sendUser, @Nullable String overrideUser, @Nullable String appName,
            @Nullable String appVersion, Map<String, String> additionalFields) {
        // make an array
        Map<String, String> additionalFieldsMap = new LinkedHashMap<>();
        additionalFieldsMap.put("Kusto." + name, version);

        if (appName == null) {
            appName = unpackLazy(DEFAULT_APPLICATION);
        }
        if (appVersion == null) {
            appVersion = NONE;
        }

        additionalFieldsMap
                .put(String.format("App.%s", escapeField(appName)), appVersion);
        if (additionalFields != null) {
            additionalFieldsMap.putAll(additionalFields);
        }

        String app = formatHeader(additionalFieldsMap);

        String user = NONE;
        if (sendUser) {
            if (overrideUser != null) {
                user = overrideUser;
            } else {
                user = unpackLazy(DEFAULT_USER);
            }
        }

        return new ClientDetails(app, user, null);
    }

    private static String getJavaVersion() {
        String version = System.getProperty("java.version");
        if (CoreUtils.isNullOrEmpty(version)) {
            return "UnknownVersion";
        }
        return version;
    }

    private static String getRuntime() {
        String runtime = System.getProperty("java.runtime.name");
        if (CoreUtils.isNullOrEmpty(runtime)) {
            runtime = System.getProperty("java.vm.name");
        }
        if (CoreUtils.isNullOrEmpty(runtime)) {
            runtime = System.getProperty("java.vendor");
        }
        if (CoreUtils.isNullOrEmpty(runtime)) {
            runtime = "UnknownRuntime";
        }
        return runtime;
    }

    public String getApplicationForTracing() {
        return applicationForTracing == null ? unpackLazy(DEFAULT_APPLICATION) : applicationForTracing;
    }

    public String getUserNameForTracing() {
        return userNameForTracing == null ? unpackLazy(DEFAULT_USER) : userNameForTracing;
    }

    public String getClientVersionForTracing() {
        return unpackLazy(DEFAULT_VERSION) + (appendedClientVersionForTracing == null ? "" : "|" + appendedClientVersionForTracing);
    }

}
