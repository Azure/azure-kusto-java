package com.microsoft.azure.kusto.ingest.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SourceInfoTest {

    @Test
    void validateBlobSourceInfo() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("");
        assertThrows(IllegalArgumentException.class, blobSourceInfo::validate);
    }

    @Test
    void validateFileSourceInfo() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("",0);
        assertThrows(IllegalArgumentException.class, fileSourceInfo::validate);
    }

    @Test
    void validateStreamSourceInfo() {
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(null);
        assertThrows(IllegalArgumentException.class, streamSourceInfo::validate);
    }
}