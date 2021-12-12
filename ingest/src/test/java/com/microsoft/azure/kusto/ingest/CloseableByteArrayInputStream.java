package com.microsoft.azure.kusto.ingest;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class CloseableByteArrayInputStream extends java.io.ByteArrayInputStream {
    private boolean closed;

    public CloseableByteArrayInputStream(byte[] buf) {
        super(buf);

        closed = false;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        return super.read(b);
    }

    @Override
    public void close() {
        closed = true;
    }
}
