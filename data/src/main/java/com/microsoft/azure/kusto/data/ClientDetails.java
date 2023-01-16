package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.commons.lang3.tuple.Pair;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ClientDetails {

    public static final String NONE = "[none]";

    private static String unpackLazy(ConcurrentInitializer<String> s) {
        try {
            return s.get();
        } catch (ConcurrentException e) {
            return NONE;
        }
    }

    private static ConcurrentInitializer<String> defaultApplication = new LazyInitializer<>() {
        @Override
        protected String initialize() {
            return UriUtils.stripFileNameFromCommandLine(System.getProperty("sun.java.command"));
        }
    };

    private static ConcurrentInitializer<String> defaultUser = new LazyInitializer<>() {
        @Override
        protected String initialize() {
            String user = System.getProperty("user.name");
            if (StringUtils.isBlank(user)) {
                user = System.getenv("USERNAME");
                String domain = System.getenv("USERDOMAIN");
                if (StringUtils.isNotBlank(domain) && StringUtils.isNotBlank(user)) {
                    user = domain + "\\" + user;
                }
            }
            return StringUtils.isNotBlank(user) ? user : "[none]";
        }
    };

    private static ConcurrentInitializer<String> defaultVersion = new LazyInitializer<>() {
        @Override
        protected String initialize() {
            return formatHeader(Arrays.asList(
                    Pair.of("Kusto.Java.Client", Utils.getPackageVersion()),
                    Pair.of(String.format("Runtime.%s", escapeField(getRuntime())), getJavaVersion()))
            );
        }
    };


    /**
     * Formats the given fields into a string that can be used as a header.
     *
     * @param args The fields to format.
     * @return The formatted string, for example: "field1:{value1}|field2:{value2}"
     */
    private static String formatHeader(Collection<Pair<String, String>> args) {
        return args.stream().filter(arg -> org.apache.commons.lang3.StringUtils.isNotBlank(arg.getKey()) && StringUtils.isNotBlank(arg.getValue()))
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
                                                     @Nullable String appVersion, Pair<String, String>... additionalFields) {
        // make an array
        List<Pair<String, String>> additionalFieldsList = new ArrayList<>();
        additionalFieldsList.add(Pair.of("Kusto." + name, version));

        if (appName == null) {
            appName = unpackLazy(defaultApplication);
        }

        if (appVersion == null) {
            appVersion = NONE;
        }

        additionalFieldsList
                .add(Pair.of(String.format("App.%s", escapeField(appName)), appVersion));
        if (additionalFields != null) {
            additionalFieldsList.addAll(Arrays.asList(additionalFields));
        }

        String app = formatHeader(additionalFieldsList);

        String user = NONE;
        if (sendUser) {
            if (overrideUser != null) {
                user = overrideUser;
            } else {
                user = unpackLazy(defaultUser);
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

    private String applicationForTracing;
    private String userNameForTracing;
    private String appendedClientVersionForTesting;

    public ClientDetails(String applicationForTracing, String userNameForTracing, String appendedClientVersionForTracing) {
        this.applicationForTracing = applicationForTracing;
        this.userNameForTracing = userNameForTracing;
        this.appendedClientVersionForTesting = appendedClientVersionForTracing;
    }

    public String getApplicationForTracing() {
        return applicationForTracing == null ? unpackLazy(defaultApplication) : applicationForTracing;
    }

    public String getUserNameForTracing() {
        return userNameForTracing == null ? unpackLazy(defaultUser) : userNameForTracing;
    }

    public String getClientVersionForTracing() {
        return unpackLazy(defaultVersion) + (appendedClientVersionForTesting == null ? "" : "|" + appendedClientVersionForTesting);
    }

}
