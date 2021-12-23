package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class UtilitiesTest {
    @Test
    @DisplayName("Convert millis to .Net timespan")
    void convertMillisToTimespan() {
        Long timeout = TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2); // 40 minutes 2 seconds
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        clientRequestProperties.setTimeoutInMilliSec(timeout);
        Assertions.assertEquals(timeout, clientRequestProperties.getTimeoutInMilliSec());
        Assertions.assertEquals(timeout, clientRequestProperties.getOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT));

        String serverTimeoutOptionStr = "01:40:02.1";
        long serverTimeoutOptionMillis = TimeUnit.HOURS.toMillis(1)
                + TimeUnit.MINUTES.toMillis(40)
                + TimeUnit.SECONDS.toMillis(2) + 100L;
        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, serverTimeoutOptionStr);
        Assertions.assertEquals(serverTimeoutOptionMillis, clientRequestProperties.getTimeoutInMilliSec());

        // If set to over MAX_TIMEOUT_MS - value should be MAX_TIMEOUT_MS
        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, "1.01:40:02.1");
        Assertions.assertEquals(ClientRequestProperties.MAX_TIMEOUT_MS,
                clientRequestProperties.getTimeoutInMilliSec());

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, "15:00");
        Assertions.assertEquals(TimeUnit.HOURS.toMillis(15), clientRequestProperties.getTimeoutInMilliSec());
    }
}
