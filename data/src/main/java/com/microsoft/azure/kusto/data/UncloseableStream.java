package com.microsoft.azure.kusto.data;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class exists to handle outside dependencies which close a stream when we don't want to.
 * It takes a stream, and simply forwards the call to all of its methods, except for the {@code close()} method - which does nothing.
 */
public class UncloseableStream extends InputStream {
    public InputStream getInnerStream() {
        return innerStream;
    }

    private final InputStream innerStream;

    public UncloseableStream(InputStream innerStream) {
        this.innerStream = innerStream;
    }

    /**
     * Explicitly does nothing, does preserving the inner stream as open
     */
    @Override
    public void close() {
        // Explicitly do nothing
    }

    @Override
    public int read() throws IOException {
        return innerStream.read();
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
        return innerStream.read(b);
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        return innerStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return innerStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return innerStream.available();
    }

    @Override
    public void mark(int readlimit) {
        innerStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        innerStream.reset();
    }

    @Override
    public boolean markSupported() {
        return innerStream.markSupported();
    }
}
