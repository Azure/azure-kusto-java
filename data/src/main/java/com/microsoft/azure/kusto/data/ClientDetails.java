package com.microsoft.azure.kusto.data;

import reactor.util.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClientDetails {

    public static final String NONE = "[none]";
    private static final ConcurrentHashMap<DefaultValues, String> defaultValues = new ConcurrentHashMap<>();

    private final String applicationForTracing;
    private final String userNameForTracing;
    private final String appendedClientVersionForTracing;

    public ClientDetails(String applicationForTracing, String userNameForTracing, String appendedClientVersionForTracing) {
        this.applicationForTracing = applicationForTracing;
        this.userNameForTracing = userNameForTracing;
        this.appendedClientVersionForTracing = appendedClientVersionForTracing;
    }

    private static String unpackLazy(DefaultValues key) {
        if(DefaultValues.DEFAULT_APPLICATION == key) {
            return defaultValues.computeIfAbsent(key,
                    k->UriUtils.stripFileNameFromCommandLine(System.getProperty("sun.java.command")));
        } else if(DefaultValues.DEFAULT_USER == key) {
            return defaultValues.computeIfAbsent(key, k -> {
                String user = System.getProperty("user.name");
                if (StringUtils.isBlank(user)) {
                    user = System.getenv("USERNAME");
                    String domain = System.getenv("USERDOMAIN");
                    if (StringUtils.isNotBlank(domain) && StringUtils.isNotBlank(user)) {
                        user = domain + "\\" + user;
                    }
                }
                return StringUtils.isNotBlank(user) ? user : NONE;
            });
        } else if(DefaultValues.DEFAULT_VERSION == key) {
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
                filter(arg -> StringUtils.isNotBlank(arg.getKey())
                        && StringUtils.isNotBlank(arg.getValue()))
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
            appName = unpackLazy(DefaultValues.DEFAULT_APPLICATION);
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
                user = unpackLazy(DefaultValues.DEFAULT_USER);
            }
        }

        return new ClientDetails(app, user, null);
    }

    private static String getJavaVersion() {
        String version = System.getProperty("java.version");
        if (StringUtils.isBlank(version)) {
            return "UnknownVersion";
        }
        return version;
    }

    private static String getRuntime() {
        String runtime = System.getProperty("java.runtime.name");
        if (StringUtils.isBlank(runtime)) {
            runtime = System.getProperty("java.vm.name");
        }
        if (StringUtils.isBlank(runtime)) {
            runtime = System.getProperty("java.vendor");
        }
        if (StringUtils.isBlank(runtime)) {
            runtime = "UnknownRuntime";
        }

        return runtime;
    }

    public String getApplicationForTracing() {
        return applicationForTracing == null ? unpackLazy(DefaultValues.DEFAULT_APPLICATION) : applicationForTracing;
    }

    public String getUserNameForTracing() {
        return userNameForTracing == null ? unpackLazy(DefaultValues.DEFAULT_USER) : userNameForTracing;
    }

    public String getClientVersionForTracing() {
        return unpackLazy(DefaultValues.DEFAULT_VERSION) + (appendedClientVersionForTracing == null ? "" : "|" + appendedClientVersionForTracing);
    }

    enum DefaultValues {
        DEFAULT_APPLICATION("defaultApplication"),
        DEFAULT_USER("defaultUser"),
        DEFAULT_VERSION("defaultVersion");

        private final String value;

        DefaultValues(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
