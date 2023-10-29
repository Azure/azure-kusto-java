// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceInfoTest {
    @Test
    void validate_BlankBlobPath_IllegalArgumentException() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("");
        assertThrows(IllegalArgumentException.class, blobSourceInfo::validate);
    }

    @Test
    void validate_BlankFilePath_IllegalArgumentException() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("", 0);
        assertThrows(IllegalArgumentException.class, fileSourceInfo::validate);
    }

    @Test
    void streamSourceInfoConstructor_StreamIsNull_NullPointerException() {
        assertThrows(NullPointerException.class, () -> new StreamSourceInfo(null));
    }
}
