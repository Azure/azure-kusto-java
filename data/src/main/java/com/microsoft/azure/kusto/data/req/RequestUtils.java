package com.microsoft.azure.kusto.data.req;

import com.azure.core.util.Context;

import java.time.Duration;

public class RequestUtils {
    public static Context contextWithTimeout(Duration timeout) {
        // See https://github.com/Azure/azure-sdk-for-java/blob/azure-core-http-netty_1.10.2/sdk/core/azure-core-http-netty/CHANGELOG.md#features-added
        return Context.NONE.addData("azure-response-timeout", timeout);
    }
}
