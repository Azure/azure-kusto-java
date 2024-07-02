package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.ManagedStreamingQueuingPolicy;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ManagedStreamingQueuingPolicyTest {

    @Test
    void shouldUseQueuedIngestion() {
        ManagedStreamingQueuingPolicy policy = ManagedStreamingQueuingPolicy.Default;

        // Test with dataSize, rawDataSize, compressed and dataFormat parameters
        // Adjust these values according to your needs
        long dataSize = 0;
        long rawDataSize = 0;
        boolean compressed = false;
        IngestionProperties.DataFormat dataFormat = IngestionProperties.DataFormat.CSV;

        boolean result = policy.shouldUseQueuedIngestion(dataSize, rawDataSize, compressed, dataFormat);

        // Assert the result
        // Adjust the expected result according to your needs
        boolean expectedResult = false;
        assertEquals(expectedResult, result);
    }
}
