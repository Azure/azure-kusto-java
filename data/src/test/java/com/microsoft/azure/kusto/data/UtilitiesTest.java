package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class UtilitiesTest {
    @Test
    @DisplayName("Convert millis to .Net timespan")
    void convertMillisToTimespan() {
        Long timeout = 40*60*1000L + 2000L; // 40 minutes 2 seconds
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        clientRequestProperties.setTimeoutInMilliSec(timeout);
        Assertions.assertEquals(timeout, clientRequestProperties.getTimeoutInMilliSec());
        Assertions.assertEquals("00.00:40:02.0", clientRequestProperties.getOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT));
        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, "1.01:40:02.1");
        clientRequestProperties.getTimeoutInMilliSec();
        Assertions.assertEquals(25 * (60 * 60 * 1000L) + 40*60*1000L + 2000L + 100L,
                clientRequestProperties.getTimeoutInMilliSec());
    }
}
